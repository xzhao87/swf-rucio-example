#!/bin/bash
# Rucio Configuration Script
# Source this file to set up your environment: source setup_rucio_env.sh

echo "Setting up Rucio environment..."

export username=<your rucio username>

# =============================================================================
# RUCIO SERVER CONFIGURATION
# =============================================================================

# ATLAS Production (example - replace with your actual servers)
export RUCIO_AUTH_HOST="https://nprucio01.sdcc.bnl.gov:443"
export RUCIO_RUCIO_HOST="https://nprucio01.sdcc.bnkl.gov:443"

# =============================================================================
# AUTHENTICATION CONFIGURATION
# =============================================================================

# Authentication type (x509, userpass, oidc)
export RUCIO_AUTH_TYPE="x509_proxy"  # Change to "userpass" or "oidc" if needed

# Your Rucio account name
export RUCIO_ACCOUNT="${username}"  # Replace with your actual account

# X.509 Certificate paths (UPDATE THESE PATHS!)
# These are typically your grid certificate files
export X509_USER_CERT="$HOME/.globus/usercert.pem"
export X509_USER_KEY="$HOME/.globus/userkey.pem"
export X509_CERT_DIR="/etc/grid-security/certificates"

# Alternative: If you have a proxy certificate
export X509_USER_PROXY="/tmp/x509up_u$(id -u)"

# =============================================================================
# WORKFLOW CONFIGURATION
# =============================================================================

# Default RSE (Replace with actual RSE you have access to)
export RUCIO_DEFAULT_RSE="Test_RSE"

# Default scope (Replace with your scope)
export RUCIO_DEFAULT_SCOPE="user.${username}"

# =============================================================================
# DEBUGGING AND MONITORING
# =============================================================================

# Enable detailed logging
export RUCIO_ENABLE_DETAILED_LOGGING="false"
export RUCIO_ENABLE_PERFORMANCE_MONITORING="true"

# Set environment name
export RUCIO_ENV="production"  # or "development"

# =============================================================================
# VERIFICATION
# =============================================================================

echo "Rucio environment configured:"
echo "  Auth Host: $RUCIO_AUTH_HOST"
echo "  Rucio Host: $RUCIO_RUCIO_HOST"
echo "  Account: $RUCIO_ACCOUNT"
echo "  Default RSE: $RUCIO_DEFAULT_RSE"
echo "  Default Scope: $RUCIO_DEFAULT_SCOPE"
echo ""
echo "Certificate status:"
if [ -f "$X509_USER_CERT" ]; then
    echo "  ✓ User cert found: $X509_USER_CERT"
else
    echo "  ✗ User cert NOT found: $X509_USER_CERT"
fi

if [ -f "$X509_USER_KEY" ]; then
    echo "  ✓ User key found: $X509_USER_KEY"
else
    echo "  ✗ User key NOT found: $X509_USER_KEY"
fi

if [ -d "$X509_CERT_DIR" ]; then
    echo "  ✓ CA cert dir found: $X509_CERT_DIR"
else
    echo "  ✗ CA cert dir NOT found: $X509_CERT_DIR"
fi

echo ""
echo "To test the configuration, run:"
echo "  source venv/bin/activate"
echo "  rucio-workflow config --validate"
