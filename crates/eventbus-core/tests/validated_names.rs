use eventbus_core::{ConsumerGroup, ConsumerName, Topic};
use serde::{de::DeserializeOwned, Serialize};

fn assert_wire_validation<T: DeserializeOwned + Serialize>(valid: &str, invalid: &[String]) {
    let json = serde_json::to_string(valid).unwrap();
    let decoded: T = serde_json::from_str(&json).unwrap();
    assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
    for value in invalid {
        let json = serde_json::to_string(value).unwrap();
        assert!(
            serde_json::from_str::<T>(&json).is_err(),
            "accepted {value:?}"
        );
    }
}

#[test]
fn deserialization_preserves_name_invariants_and_string_wire_format() {
    assert_wire_validation::<Topic>(
        "orders.created",
        &[
            String::new(),
            " \t".into(),
            "topic\nname".into(),
            "x".repeat(Topic::MAX_LEN + 1),
        ],
    );
    assert_wire_validation::<ConsumerGroup>(
        "orders-workers",
        &[
            String::new(),
            "  ".into(),
            "x".repeat(ConsumerGroup::MAX_LEN + 1),
        ],
    );
    assert_wire_validation::<ConsumerName>(
        "worker-1",
        &[
            String::new(),
            "  ".into(),
            "x".repeat(ConsumerName::MAX_LEN + 1),
        ],
    );
}
