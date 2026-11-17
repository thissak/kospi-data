#!/bin/zsh
# 코스피 수집기 설치: venv → .env → git/GitHub(비공개 kospi-data) → launchd 등록.
# 여러 번 실행해도 안전하다.
set -euo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
REPO=kospi-data
LABEL=dev.goldenlabs.kospi-collector
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
PY="${PYTHON:-python3}"

echo "[1/4] Python 가상환경과 패키지"
[ -d .venv ] || "$PY" -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements.txt

echo "[2/4] .env (KIS 키)"
if [ ! -f .env ]; then
  cp .env.example .env
  chmod 600 .env
  echo "  .env를 만들었습니다. KIS_APP_KEY, KIS_APP_SECRET을 채운 뒤 다시 실행하세요."
  exit 1
fi
grep -q '^KIS_APP_KEY=.\+' .env || { echo "  .env의 KIS_APP_KEY가 비어 있습니다."; exit 1; }

echo "[3/4] git 저장소와 GitHub 비공개 저장소 $REPO"
gh auth status >/dev/null 2>&1 || { echo "  gh 로그인이 필요합니다: gh auth login"; exit 1; }
gh auth setup-git
[ -d .git ] || git init -q -b main
LOGIN="$(gh api user -q .login)"
if [ -z "$(git config user.email || true)" ]; then
  git config user.name "$LOGIN"
  git config user.email "$(gh api user -q .id)+$LOGIN@users.noreply.github.com"
fi
if ! git remote get-url origin >/dev/null 2>&1; then
  if gh repo view "$LOGIN/$REPO" >/dev/null 2>&1; then
    git remote add origin "https://github.com/$LOGIN/$REPO.git"
  else
    gh repo create "$REPO" --private --source . --remote origin
  fi
fi
git add -A
git diff --cached --quiet || git commit -q -m "chore: collector setup"
git push -q -u origin main

echo "[4/4] launchd 등록 (평일 16:40)"
mkdir -p logs "$HOME/Library/LaunchAgents"
sed "s#__ROOT__#$ROOT#g" "launchd/$LABEL.plist" > "$PLIST"
plutil -lint "$PLIST" >/dev/null
launchctl bootout "gui/$(id -u)" "$PLIST" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"

echo
echo "설치 완료."
echo "초기 수집(약 1시간 이상):"
echo "  cd $ROOT && nohup .venv/bin/python collector.py --mode initial --push > logs/initial.out 2>&1 &"
