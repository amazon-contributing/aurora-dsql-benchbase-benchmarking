# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.


## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.


# Contributing Guidelines from Original Benchbase

# Contributing

We welcome all contributions! Please open a [pull request](https://github.com/cmu-db/benchbase/pulls). Common contributions may include:

- Adding support for a new DBMS.
- Adding more tests of existing benchmarks.
- Fixing any bugs or known issues.

## Contents

<!-- TOC -->

- [Contributing](#contributing)
    - [Contents](#contents)
    - [IDE](#ide)
    - [Adding a new DBMS](#adding-a-new-dbms)
    - [Java Development Notes](#java-development-notes)
        - [Code Style](#code-style)
        - [Compiler Warnings](#compiler-warnings)
        - [Avoid var keyword](#avoid-var-keyword)
            - [Alternatives to arrays of generics](#alternatives-to-arrays-of-generics)
            - [this-escape warnings](#this-escape-warnings)

<!-- /TOC -->

## IDE

Although you can use any IDE you prefer, there are some configurations for [VSCode](https://code.visualstudio.com/) that you may find useful included in the repository, including [Github Codespaces](https://github.com/features/codespaces) and [VSCode devcontainer](https://code.visualstudio.com/docs/remote/containers) support to automatically handle dependencies, environment setup, code formatting, and more.

## Ensure security before each commit
Configure git-secrets and run it before every commit. Make sure that secrets are not committed to Git version control.
Follow [this guide](https://github.com/awslabs/git-secrets) before making a commit and push to your branch.

## Adding a new DBMS

Please see the existing MySQL and PostgreSQL code for an example.

## Java Development Notes

### Code Style

To allow reviewers to focus more on code content and not style nits, [PR #416](https://github.com/cmu-db/benchbase/pulls/416) added support for auto formatting code at compile time according to [Google Java Style](https://google.github.io/styleguide/javaguide.html) using [google-java-format](https://github.com/google/google-java-format) and [fmt-maven-plugin](https://github.com/spotify/fmt-maven-plugin) Maven plugins.

Be sure to commit and include these changes in your PRs when submitting them so that the CI pipeline passes.

Additionally, this formatting style is included in the VSCode settings files for this repo.

### Compiler Warnings

In an effort to enforce clean, safe, maintainable code, [PR #413](https://github.com/cmu-db/benchbase/pull/413) enabled the `-Werror` and `-Xlint:all` options for the `javac` compiler.

This means that any compiler warnings will cause the build to fail.

If you are seeing a build failure due to a compiler warning, please fix the warning or (on rare occassions) add an exception to the line causing the issue.

### Avoid `var` keyword

In general, we prefer to avoid the `var` keyword in favor of explicit types.

#### Alternatives to arrays of generics

Per the [Java Language Specification](https://docs.oracle.com/javase/tutorial/java/generics/restrictions.html#createArrays), arrays of generic types are not allowed and will cause compiler warnings (e.g., `unchecked cast`)

In some cases, you can extend a generic type to create a non-generic type that can be used in an array.

For instance,

```java
// Simple generic class overload to avoid some cast warnings.
class SomeTypeList extends LinkedList<SomeType> {}

SomeTypeList[] someTypeLists = new SomeTypeList[] {
    new SomeTypeList(),
    new SomeTypeList(),
    new SomeTypeList(),
};
```

#### `this-escape` warnings

`possible 'this' escape before subclass is fully initialized`

The `this-escape` warning above is caused by passing using `this.someOverridableMethod()` in a constructor.
This could in theory cause problems with a subclass not being fully initialized when the method is called.

Since many of our classes are not designed to be subclassed, we can safely ignore this warning by marking the class as `final` rather than completely rewrite the class initialization.
