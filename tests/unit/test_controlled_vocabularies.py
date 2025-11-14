from pathlib import Path

import pytest

from pycmor.core.controlled_vocabularies import (
    CMIP6ControlledVocabularies,
    CMIP7ControlledVocabularies,
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


# ============================================================================
# CMIP7 Controlled Vocabularies Tests
# ============================================================================


@pytest.fixture
def cmip7_cv_dir():
    """Fixture pointing to the CMIP7-CVs submodule"""
    # Get the repository root
    test_file = Path(__file__)
    repo_root = test_file.parent.parent.parent
    cv_path = repo_root / "CMIP7-CVs"

    if not cv_path.exists():
        pytest.skip("CMIP7-CVs submodule not initialized")

    return cv_path


class TestCMIP7ControlledVocabularies:
    """Test suite for CMIP7 Controlled Vocabularies"""

    def test_can_create_cmip7_cv_instance(self, cmip7_cv_dir):
        """Test that we can create a CMIP7ControlledVocabularies instance"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        assert isinstance(cv, ControlledVocabularies)
        assert isinstance(cv, CMIP7ControlledVocabularies)

    def test_load_from_vendored_submodule(self):
        """Test loading from vendored submodule without specifying path"""
        try:
            cv = CMIP7ControlledVocabularies.load()
            assert isinstance(cv, CMIP7ControlledVocabularies)
        except FileNotFoundError:
            pytest.skip("CMIP7-CVs submodule not initialized")

    def test_load_from_directory(self, cmip7_cv_dir):
        """Test loading from a specific directory"""
        cv = CMIP7ControlledVocabularies.from_directory(cmip7_cv_dir)
        assert isinstance(cv, CMIP7ControlledVocabularies)
        assert len(cv) > 0

    def test_contains_experiment_data(self, cmip7_cv_dir):
        """Test that experiment data is loaded"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        assert "experiment" in cv
        assert isinstance(cv["experiment"], dict)
        assert len(cv["experiment"]) > 0

    def test_contains_project_data(self, cmip7_cv_dir):
        """Test that project-level data is loaded"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        # Check for at least one project-level CV
        assert "frequency" in cv or "license" in cv or "activity" in cv

    def test_picontrol_experiment_exists(self, cmip7_cv_dir):
        """Test that picontrol experiment is loaded correctly"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        assert "picontrol" in cv["experiment"]
        picontrol = cv["experiment"]["picontrol"]
        assert picontrol["id"] == "picontrol"
        assert "description" in picontrol
        assert "parent-experiment" in picontrol

    def test_historical_experiment_details(self, cmip7_cv_dir):
        """Test historical experiment has correct structure"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        assert "historical" in cv["experiment"]
        historical = cv["experiment"]["historical"]
        assert historical["id"] == "historical"
        assert historical["start"] == 1850
        assert historical["end"] == 2021
        assert "picontrol" in historical["parent-experiment"]

    def test_frequency_list_loaded(self, cmip7_cv_dir):
        """Test that frequency list is loaded correctly"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        assert "frequency" in cv
        frequencies = cv["frequency"]
        assert isinstance(frequencies, list)
        # Check for common frequencies
        assert "mon" in frequencies
        assert "day" in frequencies
        assert "1hr" in frequencies

    def test_experiment_has_jsonld_fields(self, cmip7_cv_dir):
        """Test that experiments have JSON-LD specific fields"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        picontrol = cv["experiment"]["picontrol"]
        # JSON-LD specific fields
        assert "@context" in picontrol
        assert "type" in picontrol
        assert isinstance(picontrol["type"], list)

    def test_print_experiment_ids_method(self, cmip7_cv_dir, capsys):
        """Test the print_experiment_ids method"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        cv.print_experiment_ids()
        captured = capsys.readouterr()
        # Should print something
        assert len(captured.out) > 0
        # Should contain at least one experiment name
        assert "picontrol" in captured.out or "historical" in captured.out

    def test_load_individual_files_method(self, cmip7_cv_dir):
        """Test the _load_individual_files static method"""
        experiment_dir = cmip7_cv_dir / "experiment"
        entries = CMIP7ControlledVocabularies._load_individual_files(experiment_dir)
        assert isinstance(entries, dict)
        assert len(entries) > 0
        assert "picontrol" in entries
        assert "historical" in entries

    def test_load_project_files_method(self, cmip7_cv_dir):
        """Test the _load_project_files static method"""
        project_dir = cmip7_cv_dir / "project"
        cv_data = CMIP7ControlledVocabularies._load_project_files(project_dir)
        assert isinstance(cv_data, dict)
        assert len(cv_data) > 0
        # Should have at least frequency
        assert "frequency" in cv_data

    def test_skips_special_files(self, cmip7_cv_dir):
        """Test that special files like @context and graph.jsonld are skipped"""
        experiment_dir = cmip7_cv_dir / "experiment"
        entries = CMIP7ControlledVocabularies._load_individual_files(experiment_dir)
        # These should not be in the entries
        assert "@context" not in entries
        assert "graph" not in entries
        assert "graph.min" not in entries

    def test_experiment_count(self, cmip7_cv_dir):
        """Test that a reasonable number of experiments are loaded"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        # CMIP7 should have at least 50+ experiments
        assert len(cv["experiment"]) >= 50

    def test_get_vendored_cv_path_method(self):
        """Test the _get_vendored_cv_path static method"""
        try:
            path = CMIP7ControlledVocabularies._get_vendored_cv_path()
            assert isinstance(path, Path)
            assert path.name == "CMIP7-CVs"
            assert path.exists()
        except FileNotFoundError:
            pytest.skip("CMIP7-CVs submodule not initialized")

    def test_multiple_experiments_have_correct_structure(self, cmip7_cv_dir):
        """Test that multiple experiments have the expected structure"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        experiments_to_check = ["picontrol", "historical", "1pctco2", "amip"]

        for exp_id in experiments_to_check:
            if exp_id in cv["experiment"]:
                exp = cv["experiment"][exp_id]
                assert "id" in exp
                assert "description" in exp
                assert "parent-experiment" in exp or "parent_experiment_id" in exp

    def test_cv_behaves_like_dict(self, cmip7_cv_dir):
        """Test that CMIP7ControlledVocabularies behaves like a dictionary"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        # Test dict-like behavior
        assert "experiment" in cv
        assert len(cv.keys()) > 0
        assert len(cv.values()) > 0
        assert len(cv.items()) > 0

    def test_access_nested_experiment_data(self, cmip7_cv_dir):
        """Test accessing nested data within experiments"""
        cv = CMIP7ControlledVocabularies.load(cmip7_cv_dir)
        historical = cv["experiment"]["historical"]

        # Test various fields
        assert historical.get("start") is not None
        assert historical.get("end") is not None
        assert isinstance(historical.get("parent-experiment"), list)
        assert historical.get("tier") is not None

    @pytest.mark.skipif(
        not Path(__file__).parent.parent.parent.joinpath("CMIP7-CVs").exists(),
        reason="CMIP7-CVs submodule not initialized",
    )
    def test_load_from_git_method(self):
        """Test loading from git (requires internet connection)"""
        pytest.skip("Skipping network test by default")
        # Uncomment to test:
        # cv = CMIP7ControlledVocabularies.load_from_git(branch="src-data")
        # assert isinstance(cv, CMIP7ControlledVocabularies)
        # assert "experiment" in cv
