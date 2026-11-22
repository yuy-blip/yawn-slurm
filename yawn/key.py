# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: Apache-2.0
"""llm-key: print your API key for the LLM gateway. Run it on the host where the gateway runs.

  llm-key          your key (made the first time you ask)
  llm-key --new    replace it with a new one
"""
import sys

from yawn import common


def main():
    path = sys.argv[sys.argv.index("--socket") + 1] if "--socket" in sys.argv else None
    print(common.ask("new" if "--new" in sys.argv else "get", path))


if __name__ == "__main__":
    main()
