import pytest

from pycmor.core.validate import PIPELINES_SCHEMA, PipelineSectionValidator


@pytest.fixture
def validator():
    return PipelineSectionValidator(PIPELINES_SCHEMA)


def test_initialize(validator):
    assert validator.schema == PIPELINES_SCHEMA


def test_is_qualname(validator):
    # Test with valid qualname
    validator._validate_is_qualname_or_script(True, "field", "os.path.join")


def test_is_qualname_error(validator):
    # Test with invalid qualname
    with pytest.raises(Exception):
        validator._validate_is_qualname_or_script(True, "field", "non.existent.module")


def test_validate(validator):
    # Test with valid document
    document = {"pipelines": [{"steps": ["os.path.join"]}]}
    assert validator.validate(document)


def test_validate_neither_steps_nor_uses(validator):
    # Test with invalid document (neither 'steps' nor 'uses' specified)
    document = {"name": "test"}
    valid_document = validator.validate(document)
    assert valid_document is False
    # with pytest.raises(
    #     Exception, match='At least one of "steps" or "uses" must be specified'
    # ):
    #     validator.validate(document)


def test_validate_error_non_qualname(validator):
    # Test with invalid pipeline configuration (invalid 'steps' qualname)
    pipelines = {"pipelines": [{"name": "test", "steps": ["non.existent.module"]}]}
    valid_document = validator.validate(pipelines)
    assert valid_document is False
    # with pytest.raises(Exception, match="Must be a valid Python qualname"):
    #     validator.validate(pipelines)
