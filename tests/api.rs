use std::{cell::Cell, rc::Rc};

use postgres_test_harness::{BoxError, DatabaseTemplate, FingerprintBuilder, TemplateSpec};

#[test]
fn derived_initializer_accepts_borrowed_non_send_state() {
    // Type-check the real public method without a server or polling its future.
    let captured = Rc::new(Cell::new(0));
    let check = |template: &DatabaseTemplate| {
        let future = template.derive(
            TemplateSpec::new(FingerprintBuilder::new("borrowed-step").finish()),
            |_| async {
                captured.set(captured.get() + 1);
                Ok::<(), BoxError>(())
            },
        );
        drop(future);
    };
    let _ = check;
    assert_eq!(captured.get(), 0);
}
