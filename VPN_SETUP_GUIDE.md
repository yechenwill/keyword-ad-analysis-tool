# VPN Setup and Troubleshooting Guide

This guide helps you ensure the Keyword Ad Analysis Tool works correctly with your VPN connection.

## 🔒 Why VPN is Required

The tool accesses a private API server (`prod-ssp-engine-private.ric1.admarketplace.net`) that is only accessible through your company's internal network. VPN connection is mandatory for:

- **Security**: Protecting internal APIs from external access
- **Authentication**: Verifying you're an authorized user
- **Network Access**: Reaching internal company resources

## 🚀 Quick Setup

### Step 1: Connect to VPN
1. Open your company's VPN client
2. Enter your credentials
3. Wait for connection to establish
4. Verify you're connected to the company network

### Step 2: Test Connection
```bash
# Run the VPN test script
python test_vpn_connection.py
```

### Step 3: Start the App
```bash
# Option 1: Use startup script (recommended)
python start_app.py

# Option 2: Direct launch
streamlit run streamlit_app.py
```

## 🛠️ Troubleshooting

### Common VPN Issues

#### ❌ "Cannot reach the API server"
**Symptoms**: Basic connectivity test fails
**Solutions**:
1. **Check VPN Status**: Ensure VPN is connected and active
2. **Reconnect VPN**: Disconnect and reconnect to VPN
3. **Check Credentials**: Verify VPN username/password
4. **Contact IT**: If credentials don't work

#### ❌ "Connection failed" during analysis
**Symptoms**: App starts but API calls fail
**Solutions**:
1. **Refresh VPN**: Reconnect to VPN
2. **Reduce Load**: Lower concurrent requests in settings
3. **Increase Timeout**: Raise timeout setting to 45-60 seconds
4. **Check Network**: Ensure stable internet connection

#### ❌ "Request timeout" errors
**Symptoms**: Requests take too long and fail
**Solutions**:
1. **VPN Speed**: Check VPN connection speed
2. **Reduce Concurrency**: Lower max concurrent requests
3. **Increase Delay**: Add more delay between requests
4. **Try Off-Peak**: Run during less busy hours

### VPN Client Issues

#### Windows VPN Issues
- **Check Windows Firewall**: Allow VPN client through firewall
- **Update VPN Client**: Ensure latest version is installed
- **Run as Administrator**: Try running VPN client as admin
- **Check Network Adapter**: Verify VPN adapter is enabled

#### macOS VPN Issues
- **Check System Preferences**: Verify VPN configuration
- **Keychain Access**: Check if credentials are stored correctly
- **Network Settings**: Ensure VPN is in network service order
- **Restart Network**: Reset network settings if needed

#### Linux VPN Issues
- **Check VPN Service**: Ensure VPN service is running
- **Network Manager**: Verify VPN connection in Network Manager
- **Routing Table**: Check if routes are properly configured
- **Firewall Rules**: Ensure VPN traffic is allowed

## 🔍 Diagnostic Tools

### VPN Test Script
The `test_vpn_connection.py` script performs comprehensive tests:

```bash
python test_vpn_connection.py
```

**Tests performed**:
1. **Basic Connectivity**: Can reach the API server
2. **API Endpoint**: API responds correctly
3. **Sample Query**: API returns expected data

### Manual Testing
You can also test manually:

```bash
# Test basic connectivity
ping prod-ssp-engine-private.ric1.admarketplace.net

# Test HTTP connectivity
curl -I http://prod-ssp-engine-private.ric1.admarketplace.net
```

## 📊 Performance Optimization

### VPN Performance Tips
1. **Choose Closest Server**: Connect to nearest VPN server
2. **Wired Connection**: Use Ethernet instead of WiFi when possible
3. **Close Other Apps**: Reduce bandwidth usage from other applications
4. **Update VPN Client**: Use latest version for best performance

### App Performance Settings
Adjust these settings based on your VPN performance:

- **Max Concurrent Requests**: Start with 5, increase if stable
- **Request Delay**: Start with 0.2s, reduce if fast
- **Request Timeout**: Start with 45s, adjust based on response times

## 🔐 Security Best Practices

### VPN Security
1. **Secure Credentials**: Never share VPN credentials
2. **Logout When Done**: Disconnect VPN when not using the tool
3. **Update Regularly**: Keep VPN client updated
4. **Report Issues**: Contact IT for security concerns

### Data Security
1. **Secure File Storage**: Store JSON files securely
2. **Clean Up**: Delete sensitive data after analysis
3. **Follow Policies**: Adhere to company data policies
4. **Report Breaches**: Report any security incidents

## 📞 Getting Help

### IT Support
For VPN-related issues:
- **VPN Credentials**: Contact IT for access
- **Client Installation**: Get help installing VPN client
- **Connection Issues**: Troubleshoot network problems
- **Security Concerns**: Report security issues

### Tool Support
For app-related issues:
- **Check This Guide**: Review troubleshooting steps
- **Run Diagnostics**: Use test scripts
- **Check Logs**: Review error messages
- **Contact Developer**: For technical issues

## 📋 Checklist

Before using the tool, verify:

- [ ] VPN client is installed and updated
- [ ] VPN credentials are working
- [ ] Connected to company VPN
- [ ] VPN test script passes all tests
- [ ] App shows green VPN status
- [ ] Ready to upload and analyze data

## 🔄 Regular Maintenance

### Weekly Checks
- [ ] Test VPN connection
- [ ] Update VPN client if needed
- [ ] Check for app updates
- [ ] Review security policies

### Monthly Tasks
- [ ] Change VPN password if required
- [ ] Review access permissions
- [ ] Update documentation
- [ ] Backup important data

---

**Remember**: VPN connection is essential for this tool to work. Always ensure you're connected before starting analysis. 