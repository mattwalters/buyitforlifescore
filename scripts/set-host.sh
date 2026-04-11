#!/usr/bin/env bash

# Usage: ./scripts/set-host.sh 127.0.0.2

if [ -z "$1" ]; then
  echo "Usage: $0 <new-host>"
  echo "Example: $0 127.0.0.2"
  exit 1
fi

NEW_HOST=$1
echo "Setting application host to $NEW_HOST..."

if [ "$(uname -s)" = "Darwin" ]; then
  # Only add alias if it's not the default localhost
  if [ "$NEW_HOST" != "127.0.0.1" ]; then
    # Check if the alias already exists on lo0
    if ! ifconfig lo0 | grep -q "inet $NEW_HOST "; then
      echo "macOS detected: Requesting password to create a loopback network alias for $NEW_HOST..."
      sudo ifconfig lo0 alias "$NEW_HOST" up
      echo "✅ Created loopback alias for $NEW_HOST"
    else
      echo "✅ Loopback alias for $NEW_HOST already exists"
    fi
  fi
fi

# Update .env
if [ -f .env ]; then
  # check if HOST= already exists
  if grep -q "^HOST=" .env; then
    sed -i '' -e "s/^HOST=.*/HOST=\"$NEW_HOST\"/" .env
  else
    echo "HOST=\"$NEW_HOST\"" | cat - .env > temp && mv temp .env
  fi
  echo "✅ Updated .env"
fi

# Update .env.test
if [ -f .env.test ]; then
  if grep -q "^HOST=" .env.test; then
    sed -i '' -e "s/^HOST=.*/HOST=\"$NEW_HOST\"/" .env.test
  else
    # Insert at top
    echo "HOST=\"$NEW_HOST\"" | cat - .env.test > temp && mv temp .env.test
  fi
  echo "✅ Updated .env.test"
fi

# Update .caddy file
if [ -f .caddy ]; then
  # Replaces 'reverse_proxy <ANY_IP>:<PORT>' with 'reverse_proxy $NEW_HOST:<PORT>'
  sed -i '' -E "s/reverse_proxy [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:([0-9]+)/reverse_proxy $NEW_HOST:\1/g" .caddy
  echo "✅ Updated .caddy"
  
  # Try to reload caddy silently if it's running
  if command -v brew &> /dev/null && brew services list | grep -q caddy; then
    brew services restart caddy &> /dev/null || true
  elif command -v caddy &> /dev/null; then
    caddy reload --config /opt/homebrew/etc/Caddyfile &> /dev/null || true
  fi
fi

echo ""
echo "🎉 Done! Your application is now configured to run on $NEW_HOST!"
echo "Make sure to completely restart any running dev servers (e.g. npm run dev) so the changes take effect."
