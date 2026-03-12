#!/bin/bash
# Script to set up autochecker user on VM
# Run this script ONCE you are connected to your VM via SSH

set -e

echo "=== Setting up autochecker user on VM ==="

# 1. Create the autochecker user
echo "Creating autochecker user..."
sudo adduser --disabled-password --gecos "" autochecker || echo "User may already exist"

# 2. Create .ssh directory
echo "Creating .ssh directory..."
sudo mkdir -p /home/autochecker/.ssh
sudo chmod 700 /home/autochecker/.ssh
sudo chown autochecker:autochecker /home/autochecker/.ssh

# 3. Add the autochecker public key
echo "Adding SSH public key..."
echo "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKiL0DDQZw7L0Uf1c9cNlREY7IS6ZkIbGVWNsClqGNCZ se-toolkit-autochecker" | sudo tee /home/autochecker/.ssh/authorized_keys

# 4. Set correct permissions
echo "Setting permissions..."
sudo chmod 600 /home/autochecker/.ssh/authorized_keys
sudo chown autochecker:autochecker /home/autochecker/.ssh/authorized_keys

echo ""
echo "=== Autochecker setup complete! ==="
echo "Testing SSH connection..."
ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes autochecker@10.93.26.104 "echo 'SSH connection successful!'" && echo "✓ SSH test passed!" || echo "✗ SSH test failed"
