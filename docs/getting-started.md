# Getting started with Codex CLI

For an overview of Codex CLI features, see [this documentation](https://developers.openai.com/codex/cli/features#running-in-interactive-mode).

Pass `--plan` to enter an interactive session in Plan mode, including its first prompt:

```sh
codex --plan "Design the change"
codex resume --last --plan
codex fork --last --plan
```

For resumed or forked sessions, `--plan` overrides the saved mode for this launch.
You can switch modes normally afterward. If the server does not provide Plan mode,
startup stops with an error before sending the prompt.
