use std::collections::BTreeSet;

use cooperators::error::ErrorCode;

#[test]
fn taxonomy_maps_to_distinct_server_reserved_jsonrpc_codes() {
    let mut codes = BTreeSet::new();
    for code in ErrorCode::ALL {
        assert_eq!(code.as_str(), code.as_str().to_ascii_uppercase());
        let rpc = code.rpc_code();
        assert!(
            (-32099..=-32000).contains(&rpc),
            "{} escaped the server-reserved range: {rpc}",
            code.as_str()
        );
        assert!(
            codes.insert(rpc),
            "duplicate rpc code for {}",
            code.as_str()
        );
        assert_eq!(ErrorCode::from_rpc_code(rpc), Some(code));
        assert_eq!(ErrorCode::from_name(code.as_str()), Some(code));
        assert_eq!(code.to_string(), code.as_str());
    }
    assert_eq!(codes.len(), 12);
    assert_eq!(ErrorCode::from_rpc_code(-32601), None);
    assert_eq!(ErrorCode::from_name("NOPE"), None);
}
