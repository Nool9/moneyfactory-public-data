const DISPATCH_URL = "https://api.github.com/repos/Nool9/moneyfactory-public-data/actions/workflows/snapshot.yml/dispatches";

export default {
  async scheduled(_controller, env) {
    if (!env.GITHUB_TOKEN) throw new Error("GITHUB_TOKEN is not configured");
    const response = await fetch(DISPATCH_URL, {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        "User-Agent": "moneyfactory-cloudflare-dispatcher",
        "X-GitHub-Api-Version": "2026-03-10",
      },
      body: JSON.stringify({ ref: "master" }),
    });
    if (!response.ok) throw new Error(`GitHub dispatch failed: ${response.status}`);
  },
};
