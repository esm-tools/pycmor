from pycmor.core.frequency import Frequency


def test_frequency_for_mon_has_name_mon():
    assert Frequency.for_name("mon").name == "mon"


def test_frequency_for_monPt_has_name_monPt():
    assert Frequency.for_name("monPt").name == "monPt"


def test_interval_for_mon_is_30():
    assert Frequency.for_name("mon").approx_interval == 30.0


def test_mon_is_sorted_before_dec():
    assert Frequency.for_name("mon") < Frequency.for_name("dec")


def test_3hr_and_3hrPt_have_same_sort_order():
    assert (
        Frequency.for_name("3hr").approx_interval
        == Frequency.for_name("3hrPt").approx_interval
    )


def test_3hr_does_not_equal_3hrPt():
    assert Frequency.for_name("3hr") != Frequency.for_name("3hrPt")


def test_3hr_is_less_than_day():
    assert Frequency.for_name("3hr") < Frequency.for_name("day")
    assert Frequency.for_name("3hrPt") < Frequency.for_name("day")
