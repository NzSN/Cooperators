//! Shared runtime support for compiler-generated fault profile bindings.
use std::collections::BTreeMap;
use std::path::Path;

use mirrorrust::{run_client_with_traces, ApalacheConfig, BindingError, FallibleStateComputer, Value};
use num_bigint::BigInt;

pub fn error(message: impl std::fmt::Display) -> BindingError {
    BindingError::new("fault_sut_error", message.to_string())
}

pub fn value(json: serde_json::Value) -> Result<Value, BindingError> {
    Ok(match json {
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::String(value) => Value::Str(value),
        serde_json::Value::Number(value) => Value::Int(value.to_string().parse::<BigInt>().map_err(error)?),
        serde_json::Value::Array(items) => Value::Seq(items.into_iter().map(value).collect::<Result<_,_>>()?),
        serde_json::Value::Object(mut fields) if fields.len() == 1 && fields.contains_key("#bigint") => {
            Value::Int(fields.remove("#bigint").unwrap().as_str().ok_or_else(|| error("invalid integer"))?.parse::<BigInt>().map_err(error)?)
        }
        serde_json::Value::Object(mut fields) if fields.len() == 1 && fields.contains_key("#set") => {
            let items = fields.remove("#set").unwrap();
            Value::Set(items.as_array().ok_or_else(|| error("invalid set"))?.iter().cloned().map(value).collect::<Result<_,_>>()?)
        }
        serde_json::Value::Object(mut fields) if fields.len() == 1 && fields.contains_key("#map") => {
            let items = fields.remove("#map").unwrap();
            let mut pairs = Vec::new();
            for pair in items.as_array().ok_or_else(|| error("invalid map"))? {
                let pair = pair.as_array().ok_or_else(|| error("invalid map entry"))?;
                if pair.len() != 2 { return Err(error("invalid map pair")); }
                pairs.push((value(pair[0].clone())?, value(pair[1].clone())?));
            }
            Value::Map(pairs)
        }
        serde_json::Value::Object(fields) => Value::Record(fields.into_iter().map(|(key,item)| Ok((key,value(item)?))).collect::<Result<BTreeMap<_,_>,BindingError>>()?),
        serde_json::Value::Null => return Err(error("unexpected null observation")),
    })
}

pub fn config(module: &str) -> ApalacheConfig {
    ApalacheConfig { spec_path: format!("{module}.tla"), init_predicate: None,
        next_predicate: None, const_init: None, invariant: "Coverage".into(),
        length_bound: 16, param_vars: Some("parameters".into()) }
}

pub fn run<B: FallibleStateComputer + 'static>(bin: &str, trace: &Path, config: ApalacheConfig, mut binding: B) -> Result<(), String> {
    let computer = move |action: &str, params: &mirrorrust::State, previous: &mirrorrust::State| {
        binding.compute(action, params, previous)
            .unwrap_or_else(|error| panic!("generated fault binding failed at {action}: {error}"))
    };
    run_client_with_traces(bin, config, vec![trace.to_string_lossy().into_owned()], computer).map_err(|error| error.to_string())
}
