#!/usr/bin/env python3
"""
Requirements installation script for simulated pipeline
"""

requirements = """
faker==20.1.0
pandas==2.1.4
requests==2.31.0
"""

with open('requirements.txt', 'w') as f:
    f.write(requirements.strip())

print("✅ Requirements file created: requirements.txt")
print("Install with: pip install -r requirements.txt")
