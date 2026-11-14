#!/usr/bin/env bash
# QA: outward MCP OAuth end to end against a throwaway home on a non-8793 port.
# Usage: KU=/path/to/ku scripts/qa-mcp-oauth.sh   (approval is simulated via the
# daemon-less file path: we flip the pending request with `ku`-free jq if no daemon.)
set -euo pipefail
KU=${KU:-ku}; PORT=${PORT:-18793}; B=http://127.0.0.1:$PORT
H=$(mktemp -d "${TMPDIR:?set TMPDIR to a writable scratch folder}/qa-oauth.XXXX")
export KURISU_HOME=$H
"$KU" mcp serve --host 127.0.0.1 --port $PORT >"$H/serve.log" 2>&1 & SP=$!
trap 'kill $SP 2>/dev/null' EXIT; sleep 1
echo "1 unauth:"; curl -si -XPOST $B/mcp -d '{}' | grep -iE '^HTTP|www-authenticate'
echo "2 discovery:"; curl -s $B/.well-known/oauth-protected-resource; echo; curl -s $B/.well-known/oauth-authorization-server/mcp; echo
RED=http://127.0.0.1:33418/callback
CID=$(curl -s -XPOST $B/register -H 'content-type: application/json' -d "{\"client_name\":\"QA\",\"redirect_uris\":[\"$RED\"]}" | jq -r .client_id)
echo "3 client_id=$CID"
VER=qa-verifier-qa-verifier-qa-verifier-qa-verifier-0123
CH=$(printf %s "$VER" | openssl dgst -sha256 -binary | base64 | tr '+/' '-_' | tr -d '=')
PAGE=$(curl -s "$B/authorize?response_type=code&client_id=$CID&redirect_uri=$RED&state=s1&code_challenge=$CH&code_challenge_method=S256&scope=mcp")
ID=$(sed -n 's/.*const id="\([0-9a-f]*\)".*/\1/p' <<<"$PAGE"); echo "4 pending id=$ID"
curl -s $B/authorize/poll/$ID; echo
# Approve: what the Telegram Approve button does (op mcp_oauth_decide). No daemon here,
# so flip the 0600 pending file the same way oauth_decide does.
CODE=$(openssl rand -hex 32); NOW=$(date +%s)
jq --arg id "$ID" --arg c "$CODE" --argjson t $((NOW+300)) '.[$id].decision="approved"|.[$id].code=$c|.[$id].code_expires_at=$t' \
  "$H/mcp-server-oauth-pending.json" > "$H/p.tmp" && mv "$H/p.tmp" "$H/mcp-server-oauth-pending.json"
echo "5 poll:"; curl -s $B/authorize/poll/$ID; echo
TOK=$(curl -s -XPOST $B/token -d "grant_type=authorization_code&code=$CODE&redirect_uri=$RED&client_id=$CID&code_verifier=$VER")
echo "6 token: $TOK" | sed 's/"access_token":"[0-9a-f]\{8\}[0-9a-f]*"/"access_token":"…"/'
AT=$(jq -r .access_token <<<"$TOK")
echo "7 tools/list:"; curl -s -XPOST $B/mcp -H "authorization: Bearer $AT" -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | jq -c '[.result.tools[].name]'
echo "8 reuse code:"; curl -s -XPOST $B/token -d "grant_type=authorization_code&code=$CODE&redirect_uri=$RED&client_id=$CID&code_verifier=$VER"; echo
echo "9 legacy path token:"; curl -s -o /dev/null -w '%{http_code}\n' -XPOST $B/mcp/$(cat $H/mcp-server-token) -d '{"jsonrpc":"2.0","id":1,"method":"ping"}'
ls -l $H/mcp-server-*.json
