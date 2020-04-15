from classes.aqua_positions import *
import pytest


longitude_math_test_cases = [
    # a + b = c
    [10, -10, 0],
    [0, -10, -10],
    [150, 30, 180],
    [150, 40, -170],
    [-30, 40, 10],
    [-150, -30, -180],
    [-150, -40, 170]
]


@pytest.mark.parametrize('test_case', longitude_math_test_cases)
def test_normalize_longitude(test_case):
    assert normalize_longitude_arithmetic(test_case[0] + test_case[1]) == test_case[2]


intl_date_line_test_cases = [
    # min_lon, max_lon, includes_prime_meridian, includes_intl_date_line
    [0, 180, True, True],
    [-170, 170, False, True],
    [-179, 179, True, False],
    [-10, 10, True, False],
    [-170, -10, True, True],
    [-180, -10, True, True],
    [-180, -10, False, True]
]


@pytest.mark.parametrize('test_case', intl_date_line_test_cases)
def test_includes_intl_date_line(test_case):
    assert includes_intl_date_line(test_case[0], test_case[1], test_case[2]) == test_case[3]


calc_longitude_angle_test_cases = [
    # min_lon, max_lon, includes_prime_meridian, angle
    [0, 10, True, 10],
    [-10, 10, True, 20],
    [-180, 180, True, 360],
    [-180, 180, False, 0],
    [-180, -10, True, 190],
    [-180, -10, False, 170],
    [-180, -0.001, True, 180.001],
    [-180, -0.001, False, 179.999]
]


@pytest.mark.parametrize('test_case', calc_longitude_angle_test_cases)
def test_calculate_longitude_angle(test_case):
    assert calculate_longitude_angle_in_degrees(test_case[0], test_case[1], test_case[2]) == test_case[3]


expand_longitude_slice_test_cases = [
    # Input: min_lon, max_lon, includes_prime_meridian, degrees_to_expand;
    # Output: expanded_min, expanded_max, expanded_includes_pm, expanded_span_degrees

    [
        0, 10, True, 10,
        (-10, 20, True, 30)
    ],
    [
        20, 40, False, 10,
        (10, 50, False, 40)
    ],
    [
        20, 60, False, 30,
        (-10, 90, True, 100)
    ],
    [
        -50, -10, False, 10,
        (-60, 0, True, 60)
    ],
    [
        -180, -170, False, 20,
        (-150, 160, False, 50)
    ]
]


@pytest.mark.parametrize('test_case', expand_longitude_slice_test_cases)
def test_selection_expansion(test_case):
    expansion_results = expand_longitude_slice_by_degrees(test_case[0], test_case[1], test_case[2], test_case[3])
    expected_results = test_case[4]
    assert expansion_results[0] == expected_results[0]
    assert expansion_results[1] == expected_results[1]
    assert expansion_results[2] == expected_results[2]
    assert expansion_results[3] == expected_results[3]

