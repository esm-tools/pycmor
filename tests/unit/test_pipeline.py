from pycmor.core.pipeline import Pipeline


def test_basic_creation():
    Pipeline()


def test_qualname_creation():
    Pipeline.from_qualname_list(
        [
            "pycmor.std_lib.generic.load_data",
            "pycmor.std_lib.units.handle_unit_conversion",
        ]
    )
