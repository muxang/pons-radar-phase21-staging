use pons_v2::calculate_curve_math;

#[test]
fn exact_progress_price_and_fdv_use_integer_math() {
    let m = calculate_curve_math(
        "800",
        "200",
        "75",
        "100",
        "2000000000000000000",
        "400000000000000000000",
        "1000000000000000000000",
        18,
        18,
    )
    .unwrap();
    assert_eq!(m.token_progress, "0.750000000000000000");
    assert_eq!(m.quote_progress, "0.750000000000000000");
    assert_eq!(m.spot_price_quote, "0.005000000000000000");
    assert_eq!(m.implied_fdv_quote, "5.000000000000000000");
}

#[test]
fn progress_can_fall_after_sell_and_graduation_is_one() {
    let before =
        calculate_curve_math("800", "200", "100", "100", "1", "1", "1000", 18, 18).unwrap();
    let after_sell =
        calculate_curve_math("800", "300", "80", "100", "1", "1", "1000", 18, 18).unwrap();
    let graduated =
        calculate_curve_math("800", "0", "100", "100", "1", "1", "1000", 18, 18).unwrap();
    assert!(after_sell.token_progress < before.token_progress);
    assert_eq!(graduated.token_progress, "1.000000000000000000");
}

#[test]
fn missing_decimals_or_invalid_invariant_fails_closed() {
    assert!(calculate_curve_math("0", "0", "0", "0", "1", "1", "1", 18, 18).is_none());
    assert!(calculate_curve_math("10", "11", "1", "2", "1", "1", "1", 18, 18).is_none());
}
