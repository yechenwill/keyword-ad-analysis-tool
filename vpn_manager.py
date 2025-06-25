#!/usr/bin/env python3
"""
VPN Manager for Streamlit App
Provides VPN connection assistance and status monitoring
"""

import subprocess
import platform
import os
import time
import json
from typing import Dict, List, Tuple, Optional

class VPNManager:
    """Manages VPN connections and provides status information"""
    
    def __init__(self):
        self.system = platform.system().lower()
        self.vpn_configs = self._load_vpn_configs()
    
    def _load_vpn_configs(self) -> Dict:
        """Load VPN configurations for different systems"""
        configs = {
            'darwin': {  # macOS
                'check_command': 'scutil --nc list',
                'connect_command': 'scutil --nc start "{vpn_name}"',
                'disconnect_command': 'scutil --nc stop "{vpn_name}"',
                'status_command': 'scutil --nc status "{vpn_name}"',
                'list_command': 'scutil --nc list'
            },
            'windows': {
                'check_command': 'netsh interface show interface',
                'connect_command': 'rasdial "{vpn_name}"',
                'disconnect_command': 'rasdial "{vpn_name}" /disconnect',
                'status_command': 'rasdial "{vpn_name}"',
                'list_command': 'netsh interface show interface'
            },
            'linux': {
                'check_command': 'nmcli connection show',
                'connect_command': 'nmcli connection up "{vpn_name}"',
                'disconnect_command': 'nmcli connection down "{vpn_name}"',
                'status_command': 'nmcli connection show "{vpn_name}"',
                'list_command': 'nmcli connection show'
            }
        }
        return configs.get(self.system, {})
    
    def get_system_info(self) -> Dict:
        """Get system information"""
        return {
            'system': self.system,
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'architecture': platform.architecture()[0]
        }
    
    def list_vpn_connections(self) -> List[Dict]:
        """List available VPN connections"""
        connections = []
        
        try:
            if self.system == 'darwin':  # macOS
                result = subprocess.run(
                    self.vpn_configs['list_command'].split(),
                    capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    for line in lines:
                        if 'Connected' in line or 'Disconnected' in line:
                            parts = line.split()
                            if len(parts) >= 2:
                                name = parts[0]
                                status = 'Connected' if 'Connected' in line else 'Disconnected'
                                connections.append({
                                    'name': name,
                                    'status': status,
                                    'type': 'VPN'
                                })
            
            elif self.system == 'windows':
                result = subprocess.run(
                    self.vpn_configs['list_command'].split(),
                    capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    for line in lines:
                        if 'VPN' in line or 'RAS' in line:
                            parts = line.split()
                            if len(parts) >= 4:
                                name = ' '.join(parts[3:])
                                status = parts[2]
                                connections.append({
                                    'name': name,
                                    'status': status,
                                    'type': 'VPN'
                                })
            
            elif self.system == 'linux':
                result = subprocess.run(
                    self.vpn_configs['list_command'].split(),
                    capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    for line in lines:
                        if 'vpn' in line.lower() or 'openvpn' in line.lower():
                            parts = line.split()
                            if len(parts) >= 2:
                                name = parts[0]
                                status = parts[1]
                                connections.append({
                                    'name': name,
                                    'status': status,
                                    'type': 'VPN'
                                })
        
        except Exception as e:
            print(f"Error listing VPN connections: {e}")
        
        return connections
    
    def connect_vpn(self, vpn_name: str, username: str = "", password: str = "") -> Tuple[bool, str]:
        """Connect to a VPN"""
        try:
            if not vpn_name:
                return False, "VPN name is required"
            
            # Check if already connected
            status, _ = self.get_vpn_status(vpn_name)
            if status:
                return True, f"Already connected to {vpn_name}"
            
            # Connect to VPN
            if self.system == 'darwin':
                cmd = self.vpn_configs['connect_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
            elif self.system == 'windows':
                cmd = self.vpn_configs['connect_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
            elif self.system == 'linux':
                cmd = self.vpn_configs['connect_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
            else:
                return False, f"Unsupported system: {self.system}"
            
            if result.returncode == 0:
                # Wait a moment for connection to establish
                time.sleep(3)
                return True, f"Successfully connected to {vpn_name}"
            else:
                return False, f"Failed to connect: {result.stderr}"
        
        except subprocess.TimeoutExpired:
            return False, "Connection attempt timed out"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def disconnect_vpn(self, vpn_name: str) -> Tuple[bool, str]:
        """Disconnect from a VPN"""
        try:
            if not vpn_name:
                return False, "VPN name is required"
            
            # Check if connected
            status, _ = self.get_vpn_status(vpn_name)
            if not status:
                return True, f"Not connected to {vpn_name}"
            
            # Disconnect from VPN
            if self.system == 'darwin':
                cmd = self.vpn_configs['disconnect_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
            elif self.system == 'windows':
                cmd = self.vpn_configs['disconnect_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
            elif self.system == 'linux':
                cmd = self.vpn_configs['disconnect_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
            else:
                return False, f"Unsupported system: {self.system}"
            
            if result.returncode == 0:
                return True, f"Successfully disconnected from {vpn_name}"
            else:
                return False, f"Failed to disconnect: {result.stderr}"
        
        except subprocess.TimeoutExpired:
            return False, "Disconnect attempt timed out"
        except Exception as e:
            return False, f"Disconnect error: {str(e)}"
    
    def get_vpn_status(self, vpn_name: str) -> Tuple[bool, str]:
        """Get status of a specific VPN connection"""
        try:
            if not vpn_name:
                return False, "VPN name is required"
            
            if self.system == 'darwin':
                cmd = self.vpn_configs['status_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=10)
            elif self.system == 'windows':
                cmd = self.vpn_configs['status_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=10)
            elif self.system == 'linux':
                cmd = self.vpn_configs['status_command'].format(vpn_name=vpn_name)
                result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=10)
            else:
                return False, f"Unsupported system: {self.system}"
            
            if result.returncode == 0:
                output = result.stdout.lower()
                if 'connected' in output or 'up' in output:
                    return True, "Connected"
                else:
                    return False, "Disconnected"
            else:
                return False, "Status unknown"
        
        except Exception as e:
            return False, f"Status check error: {str(e)}"
    
    def get_network_info(self) -> Dict:
        """Get network information"""
        info = {
            'interfaces': [],
            'default_gateway': '',
            'dns_servers': []
        }
        
        try:
            if self.system == 'darwin':
                # Get network interfaces
                result = subprocess.run(['ifconfig'], capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    current_interface = None
                    for line in lines:
                        if line and not line.startswith('\t'):
                            current_interface = line.split(':')[0]
                        elif current_interface and 'inet ' in line:
                            ip = line.split()[1]
                            info['interfaces'].append({
                                'name': current_interface,
                                'ip': ip
                            })
                
                # Get default gateway
                result = subprocess.run(['netstat', '-nr'], capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    for line in lines:
                        if 'default' in line:
                            parts = line.split()
                            if len(parts) >= 2:
                                info['default_gateway'] = parts[1]
                                break
            
            elif self.system == 'windows':
                # Get network interfaces
                result = subprocess.run(['ipconfig'], capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    current_interface = None
                    for line in lines:
                        if 'adapter' in line.lower():
                            current_interface = line.split(':')[0].strip()
                        elif current_interface and 'ipv4' in line.lower():
                            ip = line.split(':')[1].strip()
                            info['interfaces'].append({
                                'name': current_interface,
                                'ip': ip
                            })
            
            elif self.system == 'linux':
                # Get network interfaces
                result = subprocess.run(['ip', 'addr'], capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    lines = result.stdout.split('\n')
                    current_interface = None
                    for line in lines:
                        if line and not line.startswith(' '):
                            current_interface = line.split(':')[1].strip()
                        elif current_interface and 'inet ' in line:
                            ip = line.split()[1].split('/')[0]
                            info['interfaces'].append({
                                'name': current_interface,
                                'ip': ip
                            })
        
        except Exception as e:
            print(f"Error getting network info: {e}")
        
        return info
    
    def test_internal_connectivity(self) -> Dict:
        """Test connectivity to internal resources"""
        tests = {
            'api_server': {
                'host': 'prod-ssp-engine-private.ric1.admarketplace.net',
                'port': 80,
                'status': False,
                'error': ''
            }
        }
        
        for test_name, test_config in tests.items():
            try:
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((test_config['host'], test_config['port']))
                sock.close()
                
                if result == 0:
                    test_config['status'] = True
                else:
                    test_config['error'] = f"Connection failed (error code: {result})"
            
            except Exception as e:
                test_config['error'] = str(e)
        
        return tests

def create_vpn_manager() -> VPNManager:
    """Create and return a VPN manager instance"""
    return VPNManager() 