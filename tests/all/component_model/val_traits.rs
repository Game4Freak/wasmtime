use wasmtime::component::{Component, Linker, Val};
use wasmtime::{Result, Store};

/// Test that Val can be used as a parameter in typed functions
/// This demonstrates the key use case from issue #7701: using Val to allow
/// dynamic typing for specific parameters while keeping others statically typed
#[test]
fn val_as_typed_param_u32() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "echo") (param i32) (result i32)
                    local.get 0
                )
            )
            (core instance $i (instantiate $m))

            (func (export "echo") (param "a" u32) (result u32)
                (canon lift (core func $i "echo") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    // Use Val as a parameter type - this is the main use case
    let echo = instance.get_typed_func::<(Val,), (u32,)>(&mut store, "echo")?;
    let result = echo.call(&mut store, (Val::U32(42),))?;
    assert_eq!(result.0, 42);

    // Test with different value
    let result = echo.call(&mut store, (Val::U32(100),))?;
    assert_eq!(result.0, 100);

    Ok(())
}

/// Test Val with bool type
#[test]
fn val_as_typed_param_bool() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "not") (param i32) (result i32)
                    ;; Return !param (1 if 0, 0 otherwise)
                    local.get 0
                    i32.eqz
                )
            )
            (core instance $i (instantiate $m))

            (func (export "not") (param "a" bool) (result bool)
                (canon lift (core func $i "not") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    let not_func = instance.get_typed_func::<(Val,), (bool,)>(&mut store, "not")?;
    
    let result = not_func.call(&mut store, (Val::Bool(true),))?;
    assert_eq!(result.0, false);
    
    let result = not_func.call(&mut store, (Val::Bool(false),))?;
    assert_eq!(result.0, true);

    Ok(())
}

/// Test that Val works in typed function signatures
/// This demonstrates the core achievement from issue #7701: Val can now be used
/// with TypedFunc, enabling partial type safety where some parameters are dynamic
#[test]
fn val_in_typed_signatures() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "double") (param i32) (result i32)
                    local.get 0
                    local.get 0
                    i32.add
                )
            )
            (core instance $i (instantiate $m))

            (func (export "double") (param "a" u32) (result u32)
                (canon lift (core func $i "double") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    // The key achievement: Val can be used in TypedFunc signatures
    // This enables "partial bounds" where you know the arity but not all types
    let double_func = instance.get_typed_func::<(Val,), (u32,)>(&mut store, "double")?;
    
    // Can call with Val::U32
    let result = double_func.call(&mut store, (Val::U32(21),))?;
    assert_eq!(result.0, 42);

    // Can call with different values
    let result = double_func.call(&mut store, (Val::U32(50),))?;
    assert_eq!(result.0, 100);

    Ok(())
}

/// Test that type mismatches are detected at runtime
#[test]
fn val_type_mismatch_detected() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "identity") (param i32) (result i32)
                    local.get 0
                )
            )
            (core instance $i (instantiate $m))

            (func (export "identity") (param "a" u32) (result u32)
                (canon lift (core func $i "identity") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    let func = instance.get_typed_func::<(Val,), (u32,)>(&mut store, "identity")?;
    
    // Correct type should work
    let result = func.call(&mut store, (Val::U32(42),))?;
    assert_eq!(result.0, 42);
    
    // Wrong type should fail
    let result = func.call(&mut store, (Val::Bool(true),));
    assert!(result.is_err(), "Expected error when passing wrong type");
    
    // Verify error message mentions type mismatch
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(err_str.contains("type mismatch") || err_str.contains("expected"), 
            "Error should mention type mismatch: {}", err_str);

    Ok(())
}

/// Test Val with different integer types
#[test]
fn val_different_integer_types() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "echo-s32") (param i32) (result i32)
                    local.get 0
                )
                (func (export "echo-u64") (param i64) (result i64)
                    local.get 0
                )
            )
            (core instance $i (instantiate $m))

            (func (export "echo-s32") (param "a" s32) (result s32)
                (canon lift (core func $i "echo-s32") (memory $i "memory"))
            )
            (func (export "echo-u64") (param "a" u64) (result u64)
                (canon lift (core func $i "echo-u64") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    // Test with s32
    let echo_s32 = instance.get_typed_func::<(Val,), (i32,)>(&mut store, "echo-s32")?;
    let result = echo_s32.call(&mut store, (Val::S32(-42),))?;
    assert_eq!(result.0, -42);

    // Test with u64
    let echo_u64 = instance.get_typed_func::<(Val,), (u64,)>(&mut store, "echo-u64")?;
    let result = echo_u64.call(&mut store, (Val::U64(1234567890),))?;
    assert_eq!(result.0, 1234567890);

    Ok(())
}

/// Test Val as a return type from typed functions
/// Demonstrates that Val can be used to receive dynamic return values
#[test]
fn val_as_return_type() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "get-u32") (param i32) (result i32)
                    local.get 0
                    i32.const 10
                    i32.add
                )
                (func (export "get-bool") (param i32) (result i32)
                    local.get 0
                    i32.const 0
                    i32.ne
                )
            )
            (core instance $i (instantiate $m))

            (func (export "get-u32") (param "x" u32) (result u32)
                (canon lift (core func $i "get-u32") (memory $i "memory"))
            )
            (func (export "get-bool") (param "x" u32) (result bool)
                (canon lift (core func $i "get-bool") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    // Test Val as return type with u32
    let get_u32 = instance.get_typed_func::<(u32,), (Val,)>(&mut store, "get-u32")?;
    let result = get_u32.call(&mut store, (32,))?;
    assert!(matches!(result.0, Val::U32(42)));
    if let Val::U32(v) = result.0 {
        assert_eq!(v, 42);
    }

    // Test Val as return type with bool
    let get_bool = instance.get_typed_func::<(u32,), (Val,)>(&mut store, "get-bool")?;
    let result = get_bool.call(&mut store, (1,))?;
    assert!(matches!(result.0, Val::Bool(true)));
    
    let result = get_bool.call(&mut store, (0,))?;
    assert!(matches!(result.0, Val::Bool(false)));

    Ok(())
}

/// Test Val with static types in tuple results
/// Shows Val can be part of multi-value returns
#[test]
fn val_in_tuple_results() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "div-rem") (param i32) (result i32 i32)
                    local.get 0
                    i32.const 10
                    i32.div_u
                    local.get 0
                    i32.const 10
                    i32.rem_u
                )
            )
            (core instance $i (instantiate $m))

            (func (export "div-rem") (param "a" u32) (result (tuple u32 u32))
                (canon lift (core func $i "div-rem") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    // Test with Val in first position of tuple result
    let div_rem_val_first = instance.get_typed_func::<(u32,), (Val, u32)>(&mut store, "div-rem")?;
    let result = div_rem_val_first.call(&mut store, (47,))?;
    assert!(matches!(result.0, Val::U32(4)));
    assert_eq!(result.1, 7);

    // Test with Val in second position of tuple result
    let div_rem_val_second = instance.get_typed_func::<(u32,), (u32, Val)>(&mut store, "div-rem")?;
    let result = div_rem_val_second.call(&mut store, (83,))?;
    assert_eq!(result.0, 8);
    assert!(matches!(result.1, Val::U32(3)));

    Ok(())
}

/// Test mixed inputs: Val combined with static types
#[test]
fn val_mixed_inputs() -> Result<()> {
    use wasmtime_component_util::REALLOC_AND_FREE;
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "add") (param i32 i32) (result i32)
                    local.get 0
                    local.get 1
                    i32.add
                )
                {REALLOC_AND_FREE}
            )
            (core instance $i (instantiate $m))

            (func (export "add") (param "a" u32) (param "b" u32) (result u32)
                (canon lift (core func $i "add") (memory $i "memory") (realloc (func $i "realloc")))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    // Test (Val, u32) - Val first, static second
    let add_val_static = instance.get_typed_func::<(Val, u32), (u32,)>(&mut store, "add")?;
    let result = add_val_static.call(&mut store, (Val::U32(10), 20))?;
    assert_eq!(result.0, 30);

    let result = add_val_static.call(&mut store, (Val::U32(100), 50))?;
    assert_eq!(result.0, 150);

    // Test (u32, Val) - static first, Val second
    let add_static_val = instance.get_typed_func::<(u32, Val), (u32,)>(&mut store, "add")?;
    let result = add_static_val.call(&mut store, (15, Val::U32(25)))?;
    assert_eq!(result.0, 40);

    let result = add_static_val.call(&mut store, (200, Val::U32(55)))?;
    assert_eq!(result.0, 255);

    Ok(())
}

/// Test mixed results: Val combined with static types in return values
#[test]
fn val_mixed_results() -> Result<()> {
    let component = format!(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "split") (param i32) (result i32 i32)
                    local.get 0
                    i32.const 2
                    i32.div_u
                    local.get 0
                    i32.const 2
                    i32.rem_u
                )
            )
            (core instance $i (instantiate $m))

            (func (export "split") (param "a" u32) (result (tuple u32 u32))
                (canon lift (core func $i "split") (memory $i "memory"))
            )
        )"#
    );

    let engine = super::engine();
    let component = Component::new(&engine, component)?;
    let mut store = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &component)?;

    // Test (Val, u32) as results - first result is Val, second is static
    let split_val_static = instance.get_typed_func::<(u32,), (Val, u32)>(&mut store, "split")?;
    let result = split_val_static.call(&mut store, (10,))?;
    assert!(matches!(result.0, Val::U32(5)));
    assert_eq!(result.1, 0);

    let result = split_val_static.call(&mut store, (7,))?;
    assert!(matches!(result.0, Val::U32(3)));
    assert_eq!(result.1, 1);

    // Test (u32, Val) as results - first result is static, second is Val
    let split_static_val = instance.get_typed_func::<(u32,), (u32, Val)>(&mut store, "split")?;
    let result = split_static_val.call(&mut store, (20,))?;
    assert_eq!(result.0, 10);
    assert!(matches!(result.1, Val::U32(0)));

    let result = split_static_val.call(&mut store, (15,))?;
    assert_eq!(result.0, 7);
    assert!(matches!(result.1, Val::U32(1)));

    Ok(())
}
