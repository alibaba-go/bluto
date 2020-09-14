# Contributing

## Welcome
If there is an [open issue](https://github.com/alibaba-go/bluto/issues) feel free to comment that you'll work on it.  This prevents multiple people from working on the same issue.

## Documentation
[Documentation on the interfaces/classes can be found here](https://pkg.go.dev/github.com/alibaba-go/bluto)

## Development Setup
-   Requires Golang 1.13+
-   Fork the project to create a copy of it under your GitHub user
-   `git clone` that project
-   Run `go get ./...` within the parent directory
-   Write code and unit tests to ensure proper functionality is expected
-   For testing, run `ginkgo ./... `

## Setup Issues
-   If you run into any issues while getting set up with development of this project, create an issue.  Working out the small details and hiccups will help others get started in the future

## Code Style
-   Follow the same coding style (Use an IDE that supports go vet/fmt).  The project code style should look like one person has written the code for consistency reasons
-   Keep pull requests to one feature/fix
-   If you're unsure of anything, [email us](rd@alibaba.ir)

## Issues and Bugs
If you have found a problem, follow these steps:

-   Create an issue [here](https://github.com/alibaba-go/bluto/issues)
-   Give a good example on how to reproduce this issue.  Ex. When I Get `SomeKey` it returns X but it actually is Y.
-   Label it accordingly: "bug" if it's an issue with this code.  

## Security Vulnerabilities
If you have a found a security vulnerability with this package or a security vulnerability please create an [issue](https://github.com/alibaba-go/bluto/issues).  Emails will be sent out notifying [people in this org](https://github.com/orgs/alibaba-go/people) 

## Suggestions
If you have a suggestion, feel free to create an issue with the label "suggestion".  Please note that this project is more of a Redis API and nothing more.

## Pull Requests
When your change is ready, create a pull request from your repo to this repo.  The pull request should be pointing to `master`.  Assign one of the following people from [here](https://github.com/orgs/alibaba-go/people).  Pull Requests are required to successfully build before they can be merged in.  Current Git style is like `git flow` but without squashing of commits to master.

## Requirement to have pull requests accepted
-   Must include unit tests for the new functionality/fix.  Write tests that prove the functionality works, not to merely pass code coverage
-   Must pass code review
-   Must follow the same coding style