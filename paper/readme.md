# The PyMOR Paper
## README and Notes

> [Dr. Paul Gierz](mailto:paul.gierz@awi.de)<a
    id="cy-effective-orcid-url"
    class="underline"
     href="https://orcid.org/0000-0002-4512-087X"
     target="orcid.widget"
     rel="me noopener noreferrer"
     style="vertical-align: top">
     <img
        src="https://orcid.org/sites/default/files/images/orcid_16x16.png"
        style="width: 1em; margin-inline-start: 0.5em"
        alt="ORCID iD icon"/>
    </a>
> | [GitHub](https://github.com/esm-tools/pymor)
> | [PyPI](https://pypi.org/project/py-cmor/)


It would be nice to submit a paper about PyMOR, for the sake of "completeness" if nothing else. Plus, it might help us secure additional development funding.

I would like to target the [Journal of Open Source Software (JOSS)](https://joss.theoj.org/) for this paper, a lot of the ["culture" that they have about publishing software papers](https://joss.theoj.org/about) is very similar to my
own thinking.

Example paper in commit [9ef1fdd] taken from the [JOSS website's example](https://joss.readthedocs.io/en/latest/example_paper.html#example-paper)

More hints about [citation syntax](https://pandoc.org/MANUAL.html#extension-citations).

We can submit the preprint to [arxiv](https://arxiv.org/).

How to [locally build and typeset the paper](https://joss.readthedocs.io/en/latest/paper.html#checking-that-your-paper-compiles)

## [Submission Guidelines](https://joss.readthedocs.io/en/latest/submitting.html#submitting-a-paper-to-joss)

### Submission requirements

- [x] The software must be open source as per the [OSI definition](https://opensource.org/osd).
- [x] The software must be hosted at a location where users can browse the source code files, open issues, and propose code changes without manual approval of (or payment for) accounts
- [ ] The software must have an **obvious** research application.
> [@pgierz] Do we? I think so...
- [x] You must be a major contributor to the software you are submitting, and have a GitHub account to participate in the review process.
- [x] Your paper must not focus on new research results accomplished with the software.
- [ ] Your paper (`paper.md` and BibTeX files, plus any figures) must be hosted in a Git-based repository together with your software.
> [@pgierz] This is now in progress, see [PR #179](https://github.com/esm-tools/pymor/pull/179) for details.
- [ ] ~The paper may be in a short-lived branch which is never merged with the default, although if you do this, make sure this branch is _created_ from the default so that it also includes the source code of your submission.~
> [@pgierz] I think this is not necessary. The paper can live in the main repository along with everything else.

In addition, the software associated with your submission must:

- [x] Be stored in a repository that can be cloned without registration.
- [x] Be stored in a repository where the software source files are browsable online without registration.
- [x] Have an issue tracker that is readable without registration.
- [x] Permit individuals to create issues/file tickets against your repository
