#!/usr/bin/env python3
"""
Test runner script for the bluesky_tiktok_archive project.

This script provides a convenient way to run tests with various options
and configurations.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

def main():
    """Main function to run tests."""
    parser = argparse.ArgumentParser(description='Run tests for bluesky_tiktok_archive')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--coverage', '-c', action='store_true', help='Run with coverage')
    parser.add_argument('--test-file', '-f', help='Run specific test file')
    parser.add_argument('--test-function', '-t', help='Run specific test function')
    parser.add_argument('--integration', action='store_true', help='Run integration tests only')
    parser.add_argument('--unit', action='store_true', help='Run unit tests only')
    parser.add_argument('--no-skip', action='store_true', help='Do not skip slow tests')

    args = parser.parse_args()

    # Change to the project directory
    project_dir = Path(__file__).parent
    os.chdir(project_dir)

    # Build pytest command
    cmd = [sys.executable, '-m', 'pytest']

    if args.verbose:
        cmd.append('-v')

    if args.coverage:
        cmd.extend(['--cov=.', '--cov-report=html', '--cov-report=term-missing'])

    if args.test_file:
        cmd.append(f'tests/{args.test_file}')
    else:
        cmd.append('tests')

    if args.test_function:
        cmd.append('-k')
        cmd.append(args.test_function)

    if args.integration:
        cmd.append('-m')
        cmd.append('integration')
    elif args.unit:
        cmd.append('-m')
        cmd.append('unit')

    if not args.no_skip:
        cmd.append('-m')
        cmd.append('not slow')

    # Add pytest configuration
    cmd.extend(['--tb=short', '--strict-markers'])

    print(f"Running command: {' '.join(cmd)}")
    print("=" * 50)

    try:
        result = subprocess.run(cmd, check=True)
        print("=" * 50)
        print("All tests passed! ✅")
        return 0
    except subprocess.CalledProcessError as e:
        print("=" * 50)
        print(f"Tests failed with exit code {e.returncode} ❌")
        return e.returncode
    except FileNotFoundError:
        print("Error: pytest not found. Please install it with: pip install pytest")
        return 1

if __name__ == "__main__":
    sys.exit(main())
