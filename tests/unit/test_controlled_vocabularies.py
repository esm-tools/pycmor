import pytest

from pycmor.core.controlled_vocabularies import (
    CMIP6ControlledVocabularies,
    ControlledVocabularies,
)


@pytest.fixture
def cv_experiment_id(CV_dir):
    return CMIP6ControlledVocabularies([CV_dir / "CMIP6_experiment_id.json"])


def test_can_create_controlled_vocabularies_instance(cv_experiment_id):
    assert isinstance(cv_experiment_id, ControlledVocabularies)


def test_can_read_experiment_id_json(cv_experiment_id):
    assert "experiment_id" in cv_experiment_id


def test_can_read_start_year_from_experiment_id(cv_experiment_id):
    assert cv_experiment_id["experiment_id"]["highres-future"]["start_year"] == "2015"


def test_can_read_experiment_id_and_source_id_from_directory(CV_dir):
    cv = CMIP6ControlledVocabularies.from_directory(CV_dir)
    assert cv["experiment_id"]["highres-future"]["start_year"] == "2015"
    assert "experiment_id" in cv
    assert "source_id" in cv
