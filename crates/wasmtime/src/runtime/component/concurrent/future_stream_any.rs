//! Implementation of [`FutureAny`] and [`StreamAny`].

use crate::StoreContextMut;
use crate::component::concurrent::futures_and_streams::{
    self, Destination, Source, StreamResult, TransmitKind, TransmitOrigin,
};
use crate::component::concurrent::{TableId, TransmitHandle};
use crate::component::func::{LiftContext, LowerContext, bad_type_info, desc};
use crate::component::matching::InstanceType;
use crate::component::types::{self, FutureType, StreamType};
use crate::component::{
    ComponentInstanceId, ComponentType, FutureReader, Lift, Lower, StreamReader, Val,
};
use crate::store::StoreOpaque;
use crate::{AsContextMut, Result, bail, error::Context as _};
use core::any::Any;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::any::TypeId;
use std::boxed::Box;
use std::io::Cursor;
use std::vec::Vec;
use wasmtime_core::ensure;
use wasmtime_environ::component::{
    CanonicalAbiInfo, InterfaceType, TypeFutureTableIndex, TypeStreamTableIndex,
};

/// Represents a type-erased component model `future`.
///
/// This type is similar to [`ResourceAny`](crate::component::ResourceAny)
/// where it's a static guarantee that it represents a component model
/// `future`, but it does not contain any information about the underlying type
/// that is associated with this future. This is intended to be used in
/// "dynamically typed" situations where embedders may not know ahead of time
/// the type of a `future` being used by component that is loaded.
///
/// # Closing futures
///
/// A [`FutureAny`] represents a resource that is owned by a [`Store`]. Proper
/// disposal of a future requires invoking the [`FutureAny::close`] method to
/// ensure that this handle does not leak. If [`FutureAny::close`] is not
/// called then memory will not be leaked once the owning [`Store`] is dropped,
/// but the resource handle will be leaked until the [`Store`] is dropped.
///
/// [`Store`]: crate::Store
#[derive(Debug, Clone, PartialEq)]
pub struct FutureAny {
    id: TableId<TransmitHandle>,
    ty: PayloadType<FutureType>,
}

impl FutureAny {
    fn lower_to_index<T>(&self, cx: &mut LowerContext<'_, T>, ty: InterfaceType) -> Result<u32> {
        // Note that unlike `FutureReader<T>` we need to perform an extra
        // typecheck to ensure that the dynamic type of this future matches
        // what the guest we're lowering into expects. This couldn't happen
        // before this point (see the `ComponentType::typecheck` implementation
        // for this type), so do it now.
        let future_ty = match ty {
            InterfaceType::Future(payload) => payload,
            _ => bad_type_info(),
        };
        let payload = cx.types[cx.types[future_ty].ty].payload.as_ref();
        self.ty.typecheck_guest(
            &cx.instance_type(),
            payload,
            FutureType::equivalent_payload_guest,
        )?;

        // Like `FutureReader<T>`, however, lowering "just" gets a u32.
        futures_and_streams::lower_future_to_index(self.id, cx, ty)
    }

    /// Attempts to convert this [`FutureAny`] to a [`FutureReader<T>`]
    /// with a statically known type.
    ///
    /// # Errors
    ///
    /// This function will return an error if `T` does not match the type of
    /// value on this future.
    pub fn try_into_future_reader<T>(self) -> Result<FutureReader<T>>
    where
        T: ComponentType + 'static,
    {
        self.ty
            .typecheck_host::<T>(FutureType::equivalent_payload_host::<T>)?;
        Ok(FutureReader::new_(self.id))
    }

    /// Attempts to convert `reader` to a [`FutureAny`], erasing its statically
    /// known type.
    ///
    /// # Errors
    ///
    /// This function will return an error if `reader` does not belong to
    /// `store`.
    pub fn try_from_future_reader<T>(
        mut store: impl AsContextMut,
        reader: FutureReader<T>,
    ) -> Result<Self>
    where
        T: ComponentType + 'static,
    {
        let store = store.as_context_mut();
        let ty = match store.0.transmit_origin(reader.id())? {
            TransmitOrigin::Host => PayloadType::new_host::<T>(),
            TransmitOrigin::GuestFuture(id, ty) => PayloadType::new_guest_future(store.0, id, ty),
            TransmitOrigin::GuestStream(..) => bail!("not a future"),
        };
        Ok(FutureAny {
            id: reader.id(),
            ty,
        })
    }

    fn lift_from_index(cx: &mut LiftContext<'_>, ty: InterfaceType, index: u32) -> Result<Self> {
        let id = futures_and_streams::lift_index_to_future(cx, ty, index)?;
        let InterfaceType::Future(ty) = ty else {
            unreachable!()
        };
        let ty = cx.types[ty].ty;
        Ok(FutureAny {
            id,
            // Note that this future might actually be a host-originating
            // future which means that this ascription of "the type is the
            // guest" may be slightly in accurate. The guest, however, has the
            // most accurate view of what type this future has so that should
            // be reasonable to ascribe as the type here regardless.
            ty: PayloadType::Guest(FutureType::from(ty, &cx.instance_type())),
        })
    }

    /// Create a new future with the specified producer.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource table for this store is full or if
    /// [`Config::concurrency_support`] is not enabled.
    ///
    /// [`Config::concurrency_support`]: crate::Config::concurrency_support
    pub fn new<S: crate::AsContextMut>(
        mut store: S,
        producer: impl FutureAnyProducer<S::Data>,
    ) -> Result<Self> {
        ensure!(
            store.as_context().0.concurrency_support(),
            "concurrency support is not enabled"
        );

        struct Producer<P>(P);

        impl<D, P: FutureAnyProducer<D>> StreamAnyProducer<D> for Producer<P> {
            type Buffer = Option<Val>;

            fn poll_produce<'a>(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                store: StoreContextMut<D>,
                mut destination: Destination<'a, Val, Self::Buffer>,
                finish: bool,
            ) -> Poll<Result<StreamResult>> {
                // SAFETY: This is a standard pin-projection, and we never move
                // out of `self`.
                let producer = unsafe { self.map_unchecked_mut(|v| &mut v.0) };

                Poll::Ready(Ok(
                    if let Some(value) = ready!(producer.poll_produce(cx, store, finish))? {
                        destination.set_buffer(Some(value));

                        // Here we return `StreamResult::Completed` even though
                        // we've produced the last item we'll ever produce.
                        // That's because the ABI expects
                        // `ReturnCode::Completed(1)` rather than
                        // `ReturnCode::Dropped(1)`.  In any case, we won't be
                        // called again since the future will have resolved.
                        StreamResult::Completed
                    } else {
                        StreamResult::Cancelled
                    },
                ))
            }
        }

        let id = store
            .as_context_mut()
            .new_transmit_val(TransmitKind::Future, Producer(producer))?;
        // For host-originating Val futures, we use a dummy type since Val is type-erased
        let ty = PayloadType::new_host::<()>();
        Ok(FutureAny { id, ty })
    }

    pub(super) fn new_(id: TableId<TransmitHandle>) -> Self {
        Self {
            id,
            ty: PayloadType::new_host::<()>(),
        }
    }

    pub(super) fn id(&self) -> TableId<TransmitHandle> {
        self.id
    }

    /// Set the consumer that accepts the result of this future.
    ///
    /// # Errors
    ///
    /// Returns an error if this future has already been closed.
    ///
    /// # Panics
    ///
    /// Panics if this future does not belong to `store`.
    pub fn pipe<S: crate::AsContextMut>(
        self,
        mut store: S,
        consumer: impl FutureAnyConsumer<S::Data> + Unpin,
    ) -> Result<()> {
        struct Consumer<C>(C);

        impl<D: 'static, C: FutureAnyConsumer<D>> StreamAnyConsumer<D> for Consumer<C> {
            fn poll_consume(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                mut store: StoreContextMut<D>,
                mut source: Source<Val>,
                finish: bool,
            ) -> Poll<Result<StreamResult>> {
                // SAFETY: This is a standard pin-projection, and we never move
                // out of `self`.
                let consumer = unsafe { self.map_unchecked_mut(|v| &mut v.0) };

                ready!(consumer.poll_consume(
                    cx,
                    store.as_context_mut(),
                    source.reborrow(),
                    finish
                ))?;

                Poll::Ready(Ok(if source.remaining(store) == 0 {
                    // Here we return `StreamResult::Completed` even though
                    // we've consumed the last item we'll ever consume.  That's
                    // because the ABI expects `ReturnCode::Completed(1)` rather
                    // than `ReturnCode::Dropped(1)`.  In any case, we won't be
                    // called again since the future will have resolved.
                    StreamResult::Completed
                } else {
                    StreamResult::Cancelled
                }))
            }
        }

        store
            .as_context_mut()
            .set_consumer_val(self.id, TransmitKind::Future, Consumer(consumer))
    }

    /// Close this `FutureAny`.
    ///
    /// This will close this future and cause any write that happens later to
    /// returned `DROPPED`.
    ///
    /// # Errors
    ///
    /// Returns an error if this future has already been closed.
    ///
    /// # Panics
    ///
    /// Panics if the `store` does not own this future.
    pub fn close(&mut self, mut store: impl AsContextMut) -> Result<()> {
        futures_and_streams::future_close(store.as_context_mut().0, &mut self.id)
    }
}

unsafe impl ComponentType for FutureAny {
    const ABI: CanonicalAbiInfo = CanonicalAbiInfo::SCALAR4;

    type Lower = <u32 as ComponentType>::Lower;

    fn typecheck(ty: &InterfaceType, _types: &InstanceType<'_>) -> Result<()> {
        match ty {
            InterfaceType::Future(_) => Ok(()),
            other => bail!("expected `future`, found `{}`", desc(other)),
        }
    }
}

unsafe impl Lower for FutureAny {
    fn linear_lower_to_flat<T>(
        &self,
        cx: &mut LowerContext<'_, T>,
        ty: InterfaceType,
        dst: &mut MaybeUninit<Self::Lower>,
    ) -> Result<()> {
        self.lower_to_index(cx, ty)?
            .linear_lower_to_flat(cx, InterfaceType::U32, dst)
    }

    fn linear_lower_to_memory<T>(
        &self,
        cx: &mut LowerContext<'_, T>,
        ty: InterfaceType,
        offset: usize,
    ) -> Result<()> {
        self.lower_to_index(cx, ty)?
            .linear_lower_to_memory(cx, InterfaceType::U32, offset)
    }
}

unsafe impl Lift for FutureAny {
    fn linear_lift_from_flat(
        cx: &mut LiftContext<'_>,
        ty: InterfaceType,
        src: &Self::Lower,
    ) -> Result<Self> {
        let index = u32::linear_lift_from_flat(cx, InterfaceType::U32, src)?;
        Self::lift_from_index(cx, ty, index)
    }

    fn linear_lift_from_memory(
        cx: &mut LiftContext<'_>,
        ty: InterfaceType,
        bytes: &[u8],
    ) -> Result<Self> {
        let index = u32::linear_lift_from_memory(cx, InterfaceType::U32, bytes)?;
        Self::lift_from_index(cx, ty, index)
    }
}

/// Represents a type-erased component model `stream`.
///
/// This type is similar to [`ResourceAny`](crate::component::ResourceAny)
/// where it's a static guarantee that it represents a component model
/// `stream`, but it does not contain any information about the underlying type
/// that is associated with this stream. This is intended to be used in
/// "dynamically typed" situations where embedders may not know ahead of time
/// the type of a `stream` being used by component that is loaded.
///
/// # Closing streams
///
/// A [`StreamAny`] represents a resource that is owned by a [`Store`]. Proper
/// disposal of a stream requires invoking the [`StreamAny::close`] method to
/// ensure that this handle does not leak. If [`StreamAny::close`] is not
/// called then memory will not be leaked once the owning [`Store`] is dropped,
/// but the resource handle will be leaked until the [`Store`] is dropped.
///
/// [`Store`]: crate::Store
#[derive(Debug, Clone, PartialEq)]
pub struct StreamAny {
    id: TableId<TransmitHandle>,
    ty: PayloadType<StreamType>,
}

impl StreamAny {
    fn lower_to_index<T>(&self, cx: &mut LowerContext<'_, T>, ty: InterfaceType) -> Result<u32> {
        // See comments in `FutureAny::lower_to_index` for why this is
        // different from `StreamReader`'s implementation.
        let stream_ty = match ty {
            InterfaceType::Stream(payload) => payload,
            _ => bad_type_info(),
        };
        let payload = cx.types[cx.types[stream_ty].ty].payload.as_ref();
        self.ty.typecheck_guest(
            &cx.instance_type(),
            payload,
            StreamType::equivalent_payload_guest,
        )?;
        futures_and_streams::lower_stream_to_index(self.id, cx, ty)
    }

    /// Attempts to convert this [`StreamAny`] to a [`StreamReader<T>`]
    /// with a statically known type.
    ///
    /// # Errors
    ///
    /// This function will return an error if `T` does not match the type of
    /// value on this stream.
    pub fn try_into_stream_reader<T>(self) -> Result<StreamReader<T>>
    where
        T: ComponentType + 'static,
    {
        self.ty
            .typecheck_host::<T>(StreamType::equivalent_payload_host::<T>)?;
        Ok(StreamReader::new_(self.id))
    }

    /// Attempts to convert `reader` to a [`StreamAny`], erasing its statically
    /// known type.
    ///
    /// # Errors
    ///
    /// This function will return an error if `reader` does not belong to
    /// `store`.
    pub fn try_from_stream_reader<T>(
        mut store: impl AsContextMut,
        reader: StreamReader<T>,
    ) -> Result<Self>
    where
        T: ComponentType + 'static,
    {
        let store = store.as_context_mut();
        let ty = match store.0.transmit_origin(reader.id())? {
            TransmitOrigin::Host => PayloadType::new_host::<T>(),
            TransmitOrigin::GuestStream(id, ty) => PayloadType::new_guest_stream(store.0, id, ty),
            TransmitOrigin::GuestFuture(..) => bail!("not a stream"),
        };
        Ok(StreamAny {
            id: reader.id(),
            ty,
        })
    }

    fn lift_from_index(cx: &mut LiftContext<'_>, ty: InterfaceType, index: u32) -> Result<Self> {
        let id = futures_and_streams::lift_index_to_stream(cx, ty, index)?;
        let InterfaceType::Stream(ty) = ty else {
            unreachable!()
        };
        let ty = cx.types[ty].ty;
        Ok(StreamAny {
            id,
            // Note that this stream might actually be a host-originating, but
            // see the documentation in `FutureAny::lift_from_index` for why
            // this should be ok.
            ty: PayloadType::Guest(StreamType::from(ty, &cx.instance_type())),
        })
    }

    /// Create a new stream with the specified producer.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource table for this store is full or if
    /// [`Config::concurrency_support`] is not enabled.
    ///
    /// [`Config::concurrency_support`]: crate::Config::concurrency_support
    pub fn new<S: crate::AsContextMut>(
        mut store: S,
        producer: impl StreamAnyProducer<S::Data>,
    ) -> Result<Self> {
        ensure!(
            store.as_context().0.concurrency_support(),
            "concurrency support is not enabled",
        );
        let id = store
            .as_context_mut()
            .new_transmit_val(TransmitKind::Stream, producer)?;
        // For host-originating Val streams, we use a dummy type since Val is type-erased
        let ty = PayloadType::new_host::<()>();
        Ok(StreamAny { id, ty })
    }

    pub(super) fn new_(id: TableId<TransmitHandle>) -> Self {
        Self {
            id,
            ty: PayloadType::new_host::<()>(),
        }
    }

    pub(super) fn id(&self) -> TableId<TransmitHandle> {
        self.id
    }

    /// Set the consumer that accepts the items delivered to this stream.
    ///
    /// # Errors
    ///
    /// Returns an error if this stream has already been closed.
    ///
    /// # Panics
    ///
    /// Panics if this stream does not belong to `store`.
    pub fn pipe<S: crate::AsContextMut>(
        self,
        mut store: S,
        consumer: impl StreamAnyConsumer<S::Data>,
    ) -> Result<()> {
        store
            .as_context_mut()
            .set_consumer_val(self.id, TransmitKind::Stream, consumer)
    }

    /// Close this `StreamAny`.
    ///
    /// This will close this stream and cause any write that happens later to
    /// returned `DROPPED`.
    ///
    /// # Errors
    ///
    /// Returns an error if this stream has already been closed.
    ///
    /// # Panics
    ///
    /// Panics if the `store` does not own this stream.
    pub fn close(&mut self, mut store: impl AsContextMut) -> Result<()> {
        futures_and_streams::stream_close(store.as_context_mut().0, &mut self.id)
    }
}

unsafe impl ComponentType for StreamAny {
    const ABI: CanonicalAbiInfo = CanonicalAbiInfo::SCALAR4;

    type Lower = <u32 as ComponentType>::Lower;

    fn typecheck(ty: &InterfaceType, _types: &InstanceType<'_>) -> Result<()> {
        match ty {
            InterfaceType::Stream(_) => Ok(()),
            other => bail!("expected `stream`, found `{}`", desc(other)),
        }
    }
}

unsafe impl Lower for StreamAny {
    fn linear_lower_to_flat<T>(
        &self,
        cx: &mut LowerContext<'_, T>,
        ty: InterfaceType,
        dst: &mut MaybeUninit<Self::Lower>,
    ) -> Result<()> {
        self.lower_to_index(cx, ty)?
            .linear_lower_to_flat(cx, InterfaceType::U32, dst)
    }

    fn linear_lower_to_memory<T>(
        &self,
        cx: &mut LowerContext<'_, T>,
        ty: InterfaceType,
        offset: usize,
    ) -> Result<()> {
        self.lower_to_index(cx, ty)?
            .linear_lower_to_memory(cx, InterfaceType::U32, offset)
    }
}

unsafe impl Lift for StreamAny {
    fn linear_lift_from_flat(
        cx: &mut LiftContext<'_>,
        ty: InterfaceType,
        src: &Self::Lower,
    ) -> Result<Self> {
        let index = u32::linear_lift_from_flat(cx, InterfaceType::U32, src)?;
        Self::lift_from_index(cx, ty, index)
    }

    fn linear_lift_from_memory(
        cx: &mut LiftContext<'_>,
        ty: InterfaceType,
        bytes: &[u8],
    ) -> Result<Self> {
        let index = u32::linear_lift_from_memory(cx, InterfaceType::U32, bytes)?;
        Self::lift_from_index(cx, ty, index)
    }
}

#[derive(Debug, Clone)]
enum PayloadType<T> {
    Guest(T),
    Host {
        id: TypeId,
        typecheck: fn(Option<&InterfaceType>, &InstanceType<'_>) -> Result<()>,
    },
}

impl<T: PartialEq> PartialEq for PayloadType<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PayloadType::Guest(a), PayloadType::Guest(b)) => a == b,
            (PayloadType::Guest(_), _) => false,
            (PayloadType::Host { id: a_id, .. }, PayloadType::Host { id: b_id, .. }) => {
                a_id == b_id
            }
            (PayloadType::Host { .. }, _) => false,
        }
    }
}

impl PayloadType<FutureType> {
    fn new_guest_future(
        store: &StoreOpaque,
        id: ComponentInstanceId,
        ty: TypeFutureTableIndex,
    ) -> Self {
        let types = InstanceType::new(&store.component_instance(id));
        let ty = types.types[ty].ty;
        PayloadType::Guest(FutureType::from(ty, &types))
    }
}

impl PayloadType<StreamType> {
    fn new_guest_stream(
        store: &StoreOpaque,
        id: ComponentInstanceId,
        ty: TypeStreamTableIndex,
    ) -> Self {
        let types = InstanceType::new(&store.component_instance(id));
        let ty = types.types[ty].ty;
        PayloadType::Guest(StreamType::from(ty, &types))
    }
}

impl<T> PayloadType<T> {
    fn new_host<P>() -> Self
    where
        P: ComponentType + 'static,
    {
        PayloadType::Host {
            typecheck: types::typecheck_payload::<P>,
            id: TypeId::of::<P>(),
        }
    }

    fn typecheck_guest(
        &self,
        types: &InstanceType<'_>,
        payload: Option<&InterfaceType>,
        equivalent: fn(&T, &InstanceType<'_>, Option<&InterfaceType>) -> bool,
    ) -> Result<()> {
        match self {
            Self::Guest(ty) => {
                if equivalent(ty, types, payload) {
                    Ok(())
                } else {
                    bail!("future payload types differ")
                }
            }
            Self::Host { typecheck, .. } => {
                typecheck(payload, types).context("future payload types differ")
            }
        }
    }

    fn typecheck_host<P>(&self, equivalent: fn(&T) -> Result<()>) -> Result<()>
    where
        P: ComponentType + 'static,
    {
        match self {
            Self::Guest(ty) => equivalent(ty),
            Self::Host { id, .. } => {
                if *id == TypeId::of::<P>() {
                    Ok(())
                } else {
                    bail!("future payload types differ")
                }
            }
        }
    }
}

/// Represents the host-owned write end of a type-erased stream.
pub trait StreamAnyProducer<D>: Send + 'static {
    /// The `WriteBuffer` type to use when delivering items.
    type Buffer: futures_and_streams::WriteBuffer<Val> + Default;

    /// Handle a host- or guest-initiated read by delivering zero or more items
    /// to the specified destination.
    ///
    /// This is the type-erased version of [`futures_and_streams::StreamProducer`],
    /// using [`Val`] instead of a generic `Item` type.
    ///
    /// See [`futures_and_streams::StreamProducer::poll_produce`] for detailed
    /// documentation on the behavior and return values.
    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: crate::StoreContextMut<'a, D>,
        destination: Destination<'a, Val, Self::Buffer>,
        finish: bool,
    ) -> Poll<Result<StreamResult>>;

    /// Attempt to convert the specified object into a `Box<dyn Any>` which may
    /// be downcast to the specified type.
    ///
    /// The implementation must ensure that, if it returns `Ok(_)`, a downcast
    /// to the specified type is guaranteed to succeed.
    fn try_into(me: Pin<Box<Self>>, _ty: TypeId) -> Result<Box<dyn Any>, Pin<Box<Self>>> {
        Err(me)
    }
}

/// Represents the host-owned read end of a type-erased stream.
pub trait StreamAnyConsumer<D>: Send + 'static {
    /// Handle a host- or guest-initiated write by accepting zero or more items
    /// from the specified source.
    ///
    /// This is the type-erased version of [`futures_and_streams::StreamConsumer`],
    /// using [`Val`] instead of a generic `Item` type.
    ///
    /// See [`futures_and_streams::StreamConsumer::poll_consume`] for detailed
    /// documentation on the behavior and return values.
    fn poll_consume(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: crate::StoreContextMut<D>,
        source: Source<'_, Val>,
        finish: bool,
    ) -> Poll<Result<StreamResult>>;
}

/// Represents the host-owned write end of a type-erased future.
pub trait FutureAnyProducer<D>: Send + 'static {
    /// Handle a host- or guest-initiated read by producing a value.
    ///
    /// This is the type-erased version of [`futures_and_streams::FutureProducer`],
    /// using [`Val`] instead of a generic `Item` type.
    ///
    /// See [`futures_and_streams::FutureProducer::poll_produce`] for detailed
    /// documentation on the behavior and return values.
    fn poll_produce(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: crate::StoreContextMut<D>,
        finish: bool,
    ) -> Poll<Result<Option<Val>>>;
}

/// Represents the host-owned read end of a type-erased future.
pub trait FutureAnyConsumer<D>: Send + 'static {
    /// Handle a host- or guest-initiated write by consuming a value.
    ///
    /// This is the type-erased version of [`futures_and_streams::FutureConsumer`],
    /// using [`Val`] instead of a generic `Item` type.
    ///
    /// See [`futures_and_streams::FutureConsumer::poll_consume`] for detailed
    /// documentation on the behavior and return values.
    fn poll_consume(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: crate::StoreContextMut<D>,
        source: Source<'_, Val>,
        finish: bool,
    ) -> Poll<Result<()>>;
}

impl<D> StreamAnyProducer<D> for core::iter::Empty<Val> {
    type Buffer = Option<Val>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: crate::StoreContextMut<'a, D>,
        _: Destination<'a, Val, Self::Buffer>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

impl<D> StreamAnyProducer<D> for futures::stream::Empty<Val> {
    type Buffer = Option<Val>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: crate::StoreContextMut<'a, D>,
        _: Destination<'a, Val, Self::Buffer>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

impl<D> StreamAnyProducer<D> for Vec<Val> {
    type Buffer = futures_and_streams::VecBuffer<Val>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: crate::StoreContextMut<'a, D>,
        mut dst: Destination<'a, Val, Self::Buffer>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        dst.set_buffer(std::mem::take(self.get_mut()).into());
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

impl<D> StreamAnyProducer<D> for Box<[Val]> {
    type Buffer = futures_and_streams::VecBuffer<Val>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: crate::StoreContextMut<'a, D>,
        mut dst: Destination<'a, Val, Self::Buffer>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        dst.set_buffer(std::mem::take(self.get_mut()).into_vec().into());
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

impl<E, D, Fut> FutureAnyProducer<D> for Fut
where
    E: Into<crate::Error>,
    Fut: core::future::Future<Output = Result<Val, E>> + ?Sized + Send + 'static,
{
    fn poll_produce(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: crate::StoreContextMut<D>,
        finish: bool,
    ) -> Poll<Result<Option<Val>>> {
        match self.poll(cx) {
            Poll::Ready(Ok(v)) => Poll::Ready(Ok(Some(v))),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending if finish => Poll::Ready(Ok(None)),
            Poll::Pending => Poll::Pending,
        }
    }
}
