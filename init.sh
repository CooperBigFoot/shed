#!/usr/bin/env bash
set -euo pipefail

if [ $# -eq 0 ]; then
    echo "Usage: bash init.sh <project-name>"
    echo "Example: bash init.sh my-project"
    exit 1
fi

PROJECT_NAME="$1"
# Cargo package names use hyphens; convert for any underscored input
PACKAGE_NAME="${PROJECT_NAME//_/-}"

echo "Initializing project: $PACKAGE_NAME"

# Update Cargo.toml files
sed -i '' "s/myproject/$PACKAGE_NAME/g" Cargo.toml
for f in crates/*/Cargo.toml; do
    sed -i '' "s/myproject/$PACKAGE_NAME/g" "$f"
done

# Update CLAUDE.md project overview
sed -i '' "s/DESCRIBE THE PROJECT BRIEFLY/$PACKAGE_NAME/" CLAUDE.md

# Reset README
cat > README.md << EOF
# $PACKAGE_NAME
EOF

# Self-destruct
rm -- "$0"

echo "Done! Project '$PACKAGE_NAME' is ready."
echo "Run 'cargo check' to verify."
