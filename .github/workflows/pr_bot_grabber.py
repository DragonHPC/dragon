import os
import datetime
import github
import requests

GH_TO_SLACK_REF = {
    "burkemi": "burke@hpe.com",
    "yian-chen": "yian.chen@hpe.com",
    "eric-cozzi": "eric.cozzi@hpe.com",
    "zachary-crisler": "zachary.crisler@hpe.com",
    "sanian-gaffar": "sanian.gaffar@hpe.com",
    "veena-venkata-ghorakavi": "veena.venkata.ghorakavi@hpe.com",
    "mohammad-hadi": "mohammad.hadi@hpe.com",
    "nicholas-hill": "nicholas.hill@hpe.com",
    "maria-kalantzi": "maria.kalantzi@hpe.com",
    "kent-lee": "kent.lee@hpe.com",
    "mendygra": "pete.mendygral@hpe.com",
    "indira-pimpalkhare": "indira.pimpalkhare@hpe.com",
    "potts": "davin.potts@hpe.com",
    "nick-radcliffe": "nick.radcliffe@hpe.com",
    "ashish-vinodkumar": "ashish.vinodkumar@hpe.com",
    "wahlc": "colin.wahl@hpe.com",
}

SLACK_WEBHOOK = "https://hooks.slack.com/triggers/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"


class SlackPullRequestAlerter:
    def __init__(self, api_url: str, repo: str, token: str = None):
        """Establish a connection to the requested github API

        :param api_url: URL for connecting to github API where your repo lives
        :type api_url: str
        :param repo: repository we'll be querying
        :type repo: str
        :param token: personal access token for authing, if needed, defaults to None
        :type token: str, optional
        """

        if token is None:
            token_file = os.path.join(os.environ.get("HOME"), ".github_token")
            with open(token_file, "r") as f:
                token = f.read().strip()

        # Create auth
        auth = github.Auth.Token(token)
        self.g_api = github.Github(base_url=api_url, auth=auth)

        # Get repo reference
        self.repo = self.g_api.get_repo("hpe/hpc-pe-dragon-dragon")

    def find_open_prs(self):
        """Find all open PRs in repository"""

        # Get open pull requests
        pulls = self.repo.get_pulls(state="open")

        # Filter out drafts, as that is our form of grace
        pulls = [pull for pull in pulls if pull.draft is False]

        return pulls

    def _compute_creation_delta(self, pr: github.PullRequest.PullRequest):
        """Compute the delta between a PR's creation and present

        :param pr: a pull request ojbect
        :type pr: github.PullRequest.PullRequest
        """

        # Get timezone of PR creation datetime
        try:
            tz = pr.created_at.tzinfo
        except AttributeError:
            # Default to utc if none provided
            tz = datetime.timezone.utc

        # Compute delta between now and creation date
        td = datetime.datetime.now(tz=tz) - pr.created_at

        return td

    def bracket_pr_by_age(self, prs: list[github.PullRequest.PullRequest], age_qualifier: int = 8):
        """Separate PRs by age, discarding draft PRs

        :param prs: Open Pull Requests
        :type prs: List[github.PullRequest.PullRequest]
        :param age_qualifier: age in days used to define a PR as "old" or "young", defaults to 8
        :type age_qualifier: int, optional
        """

        old_prs = []
        young_prs = []

        for pr in prs:
            td = self._compute_creation_delta(pr)
            if td.days >= age_qualifier:
                old_prs.append(pr)
            else:
                young_prs.append(pr)

        return young_prs, old_prs

    def generate_slack_message(self, pr: github.PullRequest.PullRequest):
        try:
            creator = GH_TO_SLACK_REF[pr.user.login]
        except KeyError:
            creator = pr.user.login

        try:
            reviewers = [GH_TO_SLACK_REF[user.login] for user in pr.requested_reviewers]
        except KeyError:
            reviewers = []

        td = self._compute_creation_delta(pr)

        creator_msg = ""
        reviewer_msg = ""

        # Don't generate anything if the PR is merged
        if pr.merged is True:
            return None

        # Reviews are being waited on
        if reviewers:
            reviewer_msg = ", please complete your review, if you have not already done so"
            creator_msg = ", please address any comments that may have been made"

        # Requests have been made for changes
        elif pr.state == "blocked":
            creator_msg = (
                ", there is an issue blocking ability to merge. Please address and merge, close, or move to draft"
            )

        # PR is ready to be merged, just needs to be merged
        elif pr.mergeable_state == "clean" and pr.mergeable is True:
            creator_msg = ", your PR looks ready to merge or has no reviewers. Please merge or add reviewers."

        # PR is ready to be merge in principle but some test has failed that needs investigationg
        elif pr.mergeable_state in "unstable" and pr.mergeable is True:
            creator_msg = ", your PR looks ready to merge but may have failed a pipeline test. Please investigate and proceed as makes sense."

        # PR has been reviewed, but isn't mergeable
        else:
            creator_msg = (
                ", your PR has been reviewed or has no reviewers but is blocked for some reason. Please resolve."
            )
            if reviewers:
                reviewer_msg = ", make sure you are not the source of the block"

        json_msg = {
            "pr_number": str(pr.number),
            "pr_title": pr.title,
            "pr_url": pr.html_url,
            "creator_msg": creator_msg,
            "reviewer_msg": reviewer_msg,
            "creator": creator,
            "days_open": str(td.days),
        }

        # The workflow requires we give 10 reviewer entries even if blank
        n_reviewers = len(reviewers)
        for idx in range(10):
            if idx < n_reviewers:
                json_msg[f"reviewer_{idx}"] = reviewers[idx]
            else:
                # Slack workflow does a table lookup. 'null' is the key for no user specified.
                # A key in the table must exist for a given entry, otherwise, the
                # workflow breaks down
                json_msg[f"reviewer_{idx}"] = "null"

        return json_msg


if __name__ == "__main__":
    spr_alerter = SlackPullRequestAlerter(api_url="https://github.hpe.com/api/v3", repo="hpe/hpe-pe-dragon-dragon")
    open_prs = spr_alerter.find_open_prs()

    for pr in open_prs:
        slack_msg = spr_alerter.generate_slack_message(pr)
        if slack_msg:
            biz = requests.post(SLACK_WEBHOOK, json=slack_msg)
