#!/usr/bin/env bash
#
# Copy GitHub releases (including assets) from one repository to another.
#
# Usage:
#   copy_github_releases.sh <source_repo> <target_repo>
#
# Example:
#   copy_github_releases.sh gh-dskola/orion_sv_bip guardant/orion_sv_pipeline
#
# Prerequisites:
#   - gh CLI authenticated with access to both repos
#   - jq installed
#   - Tags must already exist on the target repo (push with --tags first)

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <source_repo> <target_repo>" >&2
    echo "Example: $0 gh-dskola/orion_sv_bip guardant/orion_sv_pipeline" >&2
    exit 1
fi

source_repo="$1"
target_repo="$2"

# Temporary directory for downloading release assets
asset_dir="$(mktemp -d)"
trap 'rm -rf "$asset_dir"' EXIT

echo "Copying releases from ${source_repo} to ${target_repo}..."

# Fetch all releases from the source repo
releases_json="$(gh release list --repo "$source_repo" --json tagName,name,isDraft,isPrerelease --limit 1000)"

release_count="$(echo "$releases_json" | jq 'length')"
if [[ "$release_count" -eq 0 ]]; then
    echo "No releases found on ${source_repo}."
    exit 0
fi

echo "Found ${release_count} release(s)."

# Get existing releases on target to avoid duplicates
existing_tags="$(gh release list --repo "$target_repo" --json tagName --limit 1000 | jq -r '.[].tagName')"

echo "$releases_json" | jq -c '.[]' | while read -r rel; do
    tag="$(echo "$rel" | jq -r '.tagName')"
    name="$(echo "$rel" | jq -r '.name')"
    is_draft="$(echo "$rel" | jq -r '.isDraft')"
    is_prerelease="$(echo "$rel" | jq -r '.isPrerelease')"

    # Skip if release already exists on target
    if echo "$existing_tags" | grep -qxF "$tag"; then
        echo "  Skipping ${tag} (already exists on target)"
        continue
    fi

    echo "  Copying release ${tag}..."

    # Fetch release body (not available from 'gh release list')
    body="$(gh release view "$tag" --repo "$source_repo" --json body --jq '.body')"

    # Build flags for draft/prerelease
    release_flags=()
    if [[ "$is_draft" == "true" ]]; then
        release_flags+=("--draft")
    fi
    if [[ "$is_prerelease" == "true" ]]; then
        release_flags+=("--prerelease")
    fi

    # Download assets for this release
    release_asset_dir="${asset_dir}/${tag}"
    mkdir -p "$release_asset_dir"
    if gh release download "$tag" --repo "$source_repo" --dir "$release_asset_dir" 2>/dev/null; then
        asset_files=("$release_asset_dir"/*)
        # Check if glob matched any real files
        if [[ -e "${asset_files[0]}" ]]; then
            gh release create "$tag" \
                --repo "$target_repo" \
                --title "$name" \
                --notes "$body" \
                "${release_flags[@]}" \
                "${asset_files[@]}"
        else
            gh release create "$tag" \
                --repo "$target_repo" \
                --title "$name" \
                --notes "$body" \
                "${release_flags[@]}"
        fi
    else
        # No assets to download; create release without them
        gh release create "$tag" \
            --repo "$target_repo" \
            --title "$name" \
            --notes "$body" \
            "${release_flags[@]}"
    fi

    echo "  Done: ${tag}"
done

echo "All releases copied."
