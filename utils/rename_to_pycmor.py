#!/usr/bin/env python3
"""
Script to rename the pycmor project to pycmor.

This script handles:
1. Renaming directories (src/pycmor, src/pycmor -> src/pycmor)
2. Renaming files that contain pycmor/pycmor in their names
3. Replacing content within files (pycmor/pycmor -> pycmor)
4. Handling special cases and preserving file permissions

Usage:
    python rename_to_pycmor.py [--dry-run] [--backup]

Options:
    --dry-run    Show what would be changed without making changes
    --backup     Create a backup before making changes
"""

import argparse
import os
import re
import shutil
from pathlib import Path
from typing import Dict, List, Tuple


class ProjectRenamer:
    def __init__(self, project_root: str, dry_run: bool = False, backup: bool = False):
        self.project_root = Path(project_root).resolve()
        self.dry_run = dry_run
        self.backup = backup
        self.changes_log = []

        # Patterns to replace
        self.replacements = {
            "pycmor": "pycmor",
            "PyCMOR": "PyCMOR",
            "PYCMOR": "PYCMOR",
            "pycmor.": "pycmor.",
            "PyCMOR.": "PyCMOR.",
            "PYCMOR.": "PYCMOR.",
            "from pycmor": "from pycmor",
            "import pycmor": "import pycmor",
        }

        # Files/directories to skip
        self.skip_patterns = {
            ".git",
            "__pycache__",
            ".pytest_cache",
            ".ruff_cache",
            "htmlcov",
            "build",
            "dist",
            "*.egg-info",
            ".coverage*",
            "node_modules",
            "venv",
            "env",
            ".DS_Store",
        }

        # Binary file extensions to skip content replacement
        self.binary_extensions = {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".bmp",
            ".ico",
            ".svg",
            ".pdf",
            ".zip",
            ".tar",
            ".gz",
            ".bz2",
            ".xz",
            ".exe",
            ".dll",
            ".so",
            ".dylib",
            ".nc",
            ".hdf5",
            ".h5",  # NetCDF and HDF5 files
        }

        # File extensions that should have content replaced
        self.text_extensions = {
            ".py",
            ".rst",
            ".md",
            ".txt",
            ".yaml",
            ".yml",
            ".json",
            ".toml",
            ".cfg",
            ".ini",
            ".sh",
            ".slurm",
            ".html",
            ".css",
            ".js",
        }

    def should_skip_path(self, path: Path) -> bool:
        """Check if a path should be skipped based on skip patterns."""
        path_str = str(path)
        for pattern in self.skip_patterns:
            if pattern.startswith("*"):
                if path_str.endswith(pattern[1:]):
                    return True
            elif pattern in path_str:
                return True
        return False

    def is_binary_file(self, file_path: Path) -> bool:
        """Check if a file is binary based on extension."""
        return file_path.suffix.lower() in self.binary_extensions

    def is_text_file(self, file_path: Path) -> bool:
        """Check if a file should have text content replaced."""
        if file_path.suffix.lower() in self.text_extensions:
            return True

        # For files without extension, try to detect if they're text
        if not file_path.suffix:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    f.read(1024)  # Try to read first 1KB as text
                return True
            except (UnicodeDecodeError, PermissionError):
                return False

        return False

    def create_backup(self) -> str:
        """Create a backup of the entire project."""
        if not self.backup:
            return ""

        backup_dir = self.project_root.parent / f"{self.project_root.name}_backup"
        if backup_dir.exists():
            shutil.rmtree(backup_dir)

        print(f"Creating backup at: {backup_dir}")
        shutil.copytree(
            self.project_root,
            backup_dir,
            ignore=shutil.ignore_patterns(*self.skip_patterns),
        )
        return str(backup_dir)

    def replace_content_in_file(self, file_path: Path) -> bool:
        """Replace content in a single file. Returns True if changes were made."""
        if self.should_skip_path(file_path) or self.is_binary_file(file_path):
            return False

        if not self.is_text_file(file_path):
            return False

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        except (UnicodeDecodeError, PermissionError) as e:
            print(f"Warning: Could not read {file_path}: {e}")
            return False

        original_content = content

        # Apply replacements
        for old_name, new_name in self.replacements.items():
            # Use word boundaries to avoid partial matches
            pattern = r"\b" + re.escape(old_name) + r"\b"
            content = re.sub(pattern, new_name, content)

            # Also replace in import statements and module paths
            content = content.replace(f"from {old_name}", f"from {new_name}")
            content = content.replace(f"import {old_name}", f"import {new_name}")
            content = content.replace(f"{old_name}.", f"{new_name}.")

        if content != original_content:
            if not self.dry_run:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)

            self.changes_log.append(f"Content updated: {file_path}")
            return True

        return False

    def get_new_path_name(self, path: Path) -> Path:
        """Get the new path name with replacements applied."""
        path_str = str(path)
        new_path_str = path_str

        for old_name, new_name in self.replacements.items():
            new_path_str = new_path_str.replace(old_name, new_name)

        return Path(new_path_str)

    def rename_files(self) -> List[Tuple[Path, Path]]:
        """Rename files that contain pycmor in their names."""
        renames = []

        # Find all files that need renaming
        for root, dirs, files in os.walk(self.project_root):
            root_path = Path(root)

            if self.should_skip_path(root_path):
                continue

            for file_name in files:
                file_path = root_path / file_name

                if self.should_skip_path(file_path):
                    continue

                new_file_path = self.get_new_path_name(file_path)

                if file_path != new_file_path:
                    if not file_path.exists():
                        print(f"Warning: Source file does not exist: {file_path}")
                        continue
                    renames.append((file_path, new_file_path))

        # Perform renames
        for old_path, new_path in renames:
            try:
                if not self.dry_run:
                    # Ensure parent directory exists
                    new_path.parent.mkdir(parents=True, exist_ok=True)

                    if new_path.exists():
                        print(f"Warning: Target file already exists: {new_path}")
                        continue
                    if not old_path.parent.exists():
                        print(
                            f"Warning: Parent directory does not exist: {old_path.parent}"
                        )
                        continue

                    old_path.rename(new_path)

                self.changes_log.append(f"File renamed: {old_path} -> {new_path}")
            except Exception as e:
                print(f"Error renaming {old_path} to {new_path}: {e}")
                continue

        return renames

    def rename_directories(self) -> List[Tuple[Path, Path]]:
        """Rename directories that contain pycmor in their names."""
        renames = []

        # Find all directories that need renaming (walk top-down=False to rename leaves first)
        for root, dirs, files in os.walk(self.project_root, topdown=False):
            root_path = Path(root)

            if self.should_skip_path(root_path):
                continue

            for dir_name in dirs:
                dir_path = root_path / dir_name

                if self.should_skip_path(dir_path):
                    continue

                new_dir_path = self.get_new_path_name(dir_path)

                if dir_path != new_dir_path:
                    renames.append((dir_path, new_dir_path))

        # Perform renames
        for old_path, new_path in renames:
            try:
                if not self.dry_run:
                    # Ensure parent directory exists
                    new_path.parent.mkdir(parents=True, exist_ok=True)

                    if new_path.exists():
                        print(f"Warning: Target directory already exists: {new_path}")
                        continue
                    if not old_path.exists():
                        print(f"Warning: Source directory does not exist: {old_path}")
                        continue

                    old_path.rename(new_path)

                self.changes_log.append(f"Directory renamed: {old_path} -> {new_path}")
            except Exception as e:
                print(f"Error renaming directory {old_path} to {new_path}: {e}")
                continue

        return renames

    def update_file_contents(self) -> int:
        """Update content in all text files."""
        updated_count = 0

        for root, dirs, files in os.walk(self.project_root):
            root_path = Path(root)

            if self.should_skip_path(root_path):
                continue

            for file_name in files:
                file_path = root_path / file_name

                if self.replace_content_in_file(file_path):
                    updated_count += 1

        return updated_count

    def clean_build_artifacts(self):
        """Clean build artifacts that might contain old names."""
        artifacts_to_clean = [
            "build",
            "dist",
            "htmlcov",
            "*.egg-info",
            "__pycache__",
            ".pytest_cache",
            ".ruff_cache",
        ]

        for pattern in artifacts_to_clean:
            if pattern.startswith("*"):
                # Handle glob patterns
                for path in self.project_root.glob(f"**/{pattern}"):
                    if path.exists():
                        if not self.dry_run:
                            if path.is_dir():
                                shutil.rmtree(path)
                            else:
                                path.unlink()
                        self.changes_log.append(f"Cleaned: {path}")
            else:
                path = self.project_root / pattern
                if path.exists():
                    if not self.dry_run:
                        if path.is_dir():
                            shutil.rmtree(path)
                        else:
                            path.unlink()
                    self.changes_log.append(f"Cleaned: {path}")

    def run(self) -> Dict:
        """Run the complete renaming process."""
        print(
            f"{'DRY RUN: ' if self.dry_run else ''}Renaming project from pycmor to pycmor"
        )
        print(f"Project root: {self.project_root}")

        # Create backup if requested
        backup_path = ""
        if self.backup and not self.dry_run:
            backup_path = self.create_backup()

        # Step 1: Clean build artifacts
        print("\n1. Cleaning build artifacts...")
        self.clean_build_artifacts()

        # Step 2: Update file contents first (before renaming files/dirs)
        print("\n2. Updating file contents...")
        updated_files = self.update_file_contents()
        print(f"Updated content in {updated_files} files")

        # Step 3: Rename files
        print("\n3. Renaming files...")
        file_renames = self.rename_files()
        print(f"Renamed {len(file_renames)} files")

        # Step 4: Rename directories
        print("\n4. Renaming directories...")
        dir_renames = self.rename_directories()
        print(f"Renamed {len(dir_renames)} directories")

        # Summary
        summary = {
            "dry_run": self.dry_run,
            "backup_path": backup_path,
            "updated_files": updated_files,
            "renamed_files": len(file_renames),
            "renamed_directories": len(dir_renames),
            "total_changes": len(self.changes_log),
        }

        print(f"\n{'DRY RUN ' if self.dry_run else ''}SUMMARY:")
        print(f"- Updated content in {updated_files} files")
        print(f"- Renamed {len(file_renames)} files")
        print(f"- Renamed {len(dir_renames)} directories")
        print(f"- Total changes: {len(self.changes_log)}")

        if backup_path:
            print(f"- Backup created at: {backup_path}")

        return summary

    def save_changes_log(self, log_file: str = "rename_changes.log"):
        """Save the changes log to a file."""
        log_path = self.project_root / log_file
        with open(log_path, "w") as f:
            f.write(f"Project rename log - {self.project_root}\n")
            f.write("=" * 50 + "\n\n")
            for change in self.changes_log:
                f.write(f"{change}\n")
        print(f"Changes log saved to: {log_path}")


def main():
    parser = argparse.ArgumentParser(description="Rename pycmor project to pycmor")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without making changes",
    )
    parser.add_argument(
        "--backup", action="store_true", help="Create a backup before making changes"
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Path to project root (default: current directory)",
    )

    args = parser.parse_args()

    # Validate project root
    project_root = Path(args.project_root).resolve()
    if not project_root.exists():
        print(f"Error: Project root does not exist: {project_root}")
        return 1

    # Check if this looks like the pycmor project
    src_dir = project_root / "src"
    if not src_dir.exists():
        print(f"Warning: No 'src' directory found in {project_root}")
        print("Are you sure this is the pycmor project root?")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != "y":
            return 1

    # Run the renamer
    renamer = ProjectRenamer(project_root, args.dry_run, args.backup)

    try:
        renamer.run()

        # Save changes log
        if not args.dry_run:
            renamer.save_changes_log()

        print("\nRename completed successfully!")

        if not args.dry_run:
            print("\nNext steps:")
            print("1. Review the changes log")
            print("2. Test the renamed project")
            print("3. Update any external references (CI, documentation links, etc.)")
            print("4. Consider updating the git remote URL if needed")

        return 0

    except Exception as e:
        print(f"Error during rename: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
