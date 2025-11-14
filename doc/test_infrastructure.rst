=======================================
Test Infrastructure as Code (Testground)
=======================================

Overview
--------

The ``pycmor`` test suite runs in containerized environments defined by ``Dockerfile.test``.
These containers, called **testgrounds**, are published to GitHub Container Registry (GHCR)
to enable reproducible testing and easy access to test environments.

This approach treats test infrastructure as code: the Dockerfile is the declarative specification,
and the resulting container images are the infrastructure artifacts.

Why Testgrounds?
----------------

**Reproducibility**
  Pull the exact test environment used for any commit or release

**Efficiency**
  Pre-built images speed up CI runs and local development

**Consistency**
  Everyone tests against the same environment

**Traceability**
  Tag scheme makes it easy to find the right environment

Architecture
------------

Container Image Tagging Scheme
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Images are published to ``ghcr.io/esm-tools/pycmor-testground`` with the following naming pattern:

.. code-block:: text

    ghcr.io/esm-tools/pycmor-testground:py<version>-<identifier>

Where:

* ``<version>``: Python version (3.9, 3.10, 3.11, 3.12)
* ``<identifier>``: Either:

  * ``<commit-sha>``: Full Git commit SHA (for exact reproducibility)
  * ``<branch-name>``: Branch name (for latest on that branch)
  * ``v<semver>``: Semantic version tag (for releases, future feature)

Examples
^^^^^^^^

Get the testground for Python 3.10 from a specific commit:

.. code-block:: bash

    docker pull ghcr.io/esm-tools/pycmor-testground:py3.10-a7f2c1b...

Get the latest testground for Python 3.10 on the ``prep-release`` branch:

.. code-block:: bash

    docker pull ghcr.io/esm-tools/pycmor-testground:py3.10-prep-release

Get the testground for Python 3.10 from version 1.1.0 (future):

.. code-block:: bash

    docker pull ghcr.io/esm-tools/pycmor-testground:py3.10-v1.1.0

CI/CD Workflow
--------------

Build Process
^^^^^^^^^^^^^

On every push, the CI workflow:

1. **Authenticates** with GitHub Container Registry using ``GITHUB_TOKEN``
2. **Builds** Docker images for each Python version (3.9-3.12)
3. **Tags** each image with:

   * Commit SHA tag: ``py3.X-${{ github.sha }}``
   * Branch/ref tag: ``py3.X-${{ github.ref_name }}``

4. **Pushes** images to GHCR
5. **Exports** images as tar archives for immediate use in test jobs
6. **Uploads** tar archives as workflow artifacts
7. **Caches** tar archives for faster subsequent runs

Test Consumption
^^^^^^^^^^^^^^^^

Test jobs:

1. **Restore** the cached Docker image tar file
2. **Load** the image into Docker
3. **Run** tests inside the container
4. **Upload** coverage reports as artifacts

This approach means:

* Images are built once and reused across all test jobs
* Each test suite runs in the same environment
* Images are available in GHCR for future use

Using Testgrounds Locally
--------------------------

Pull a Testground
^^^^^^^^^^^^^^^^^

To run tests locally in the same environment as CI:

.. code-block:: bash

    # Get the latest from your current branch
    git rev-parse --abbrev-ref HEAD  # Get your branch name
    docker pull ghcr.io/esm-tools/pycmor-testground:py3.10-<branch-name>

    # Or get a specific commit
    docker pull ghcr.io/esm-tools/pycmor-testground:py3.10-<commit-sha>

Run Tests in Testground
^^^^^^^^^^^^^^^^^^^^^^^^

Mount your local code and run tests:

.. code-block:: bash

    docker run --rm \
      -v $(pwd):/workspace \
      ghcr.io/esm-tools/pycmor-testground:py3.10-prep-release \
      bash -c "cd /workspace && pytest"

Build a Testground Locally
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To build the testground yourself (useful when modifying ``Dockerfile.test``):

.. code-block:: bash

    docker build \
      -f Dockerfile.test \
      --build-arg PYTHON_VERSION=3.10 \
      -t pycmor-testground:py3.10-local \
      .

Future Improvements
-------------------

Planned enhancements to reduce registry spam and improve efficiency:

Conditional Publishing
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

    # Only push to registry on main/release branches
    push: ${{ github.event_name != 'pull_request' }}

This will:

* **On PR push**: Build and cache, but don't push to GHCR
* **On merge to main**: Push with branch tag
* **On git tag/release**: Push with semver tag + update ``latest``

Cleanup Policy
^^^^^^^^^^^^^^

Implement a cleanup policy to remove old development images:

* Keep all release tags (``v*``) forever
* Keep main branch tags for 90 days
* Keep commit SHA tags for 30 days
* Keep PR branch tags for 7 days

Multi-Architecture Support
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Build images for both AMD64 and ARM64:

.. code-block:: yaml

    platforms: linux/amd64,linux/arm64

This enables testing on Apple Silicon Macs natively.

Infrastructure as Code Principles
----------------------------------

This testground system follows Infrastructure as Code (IaC) principles:

**Declarative Specification**
  ``Dockerfile.test`` declares the exact environment

**Version Control**
  Dockerfile is in Git, versioned alongside code

**Reproducibility**
  Same Dockerfile + same base image = same environment

**Automation**
  CI builds and publishes automatically

**Immutability**
  Images are immutable; changes require new builds

**Traceability**
  Tags link images to specific code versions

Troubleshooting
---------------

Image Pull Fails
^^^^^^^^^^^^^^^^

If you can't pull from GHCR:

1. Ensure you're authenticated:

   .. code-block:: bash

       echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin

2. Check package visibility settings (must be public or you need read access)

3. Verify the tag exists:

   .. code-block:: bash

       gh api /orgs/esm-tools/packages/container/pycmor-testground/versions

Old Images Not Updating
^^^^^^^^^^^^^^^^^^^^^^^^

Branch tags are updated on each push. If you have an old version:

.. code-block:: bash

    docker pull ghcr.io/esm-tools/pycmor-testground:py3.10-prep-release
    # This always gets the latest

If still old, clear local cache:

.. code-block:: bash

    docker rmi ghcr.io/esm-tools/pycmor-testground:py3.10-prep-release
    docker pull ghcr.io/esm-tools/pycmor-testground:py3.10-prep-release

Related Documentation
---------------------

* :doc:`developer_guide` - Main developer guide
* :doc:`developer_setup` - Setting up development environment
* `GitHub Container Registry docs <https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry>`_
* `Docker build-push-action <https://github.com/docker/build-push-action>`_
