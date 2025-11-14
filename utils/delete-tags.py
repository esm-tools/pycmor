#!/usr/bin/env python3
"""
This script can be used to delete (already pushed) tags from a remote repository.
"""

import subprocess
import sys
from typing import List

import questionary


def get_all_tags() -> List[str]:
    """Get all git tags from the repository."""
    result = subprocess.run(["git", "tag", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error getting tags: {result.stderr}")
        sys.exit(1)
    return [tag for tag in result.stdout.strip().split("\n") if tag]


def delete_tag(tag: str, push: bool = True) -> bool:
    """Delete a tag locally and optionally from the remote.

    Args:
        tag: The tag to delete
        push: Whether to also delete the tag from the remote

    Returns:
        bool: True if successful, False otherwise
    """
    # Delete locally
    result = subprocess.run(["git", "tag", "-d", tag], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error deleting tag {tag} locally: {result.stderr}")
        return False

    print(f"Deleted tag {tag} locally")

    # Push deletion to remote if requested
    if push:
        result = subprocess.run(
            ["git", "push", "origin", f":refs/tags/{tag}"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"Error pushing tag deletion for {tag}: {result.stderr}")
            return False
        print(f"Deleted tag {tag} from remote")

    return True


def main():
    """Main function to run the script."""
    # Get all tags
    tags = get_all_tags()

    if not tags:
        print("No tags found in the repository.")
        sys.exit(0)

    # Let user select tags to delete
    selected_tags = questionary.checkbox("Select tags to delete:", choices=tags).ask()

    if not selected_tags:
        print("No tags selected for deletion.")
        sys.exit(0)

    # Confirm deletion
    confirm = questionary.confirm(
        f"Are you sure you want to delete {len(selected_tags)} tag(s)? This cannot be undone!"
    ).ask()

    if not confirm:
        print("Deletion cancelled.")
        sys.exit(0)

    # Delete selected tags
    success_count = 0
    for tag in selected_tags:
        if delete_tag(tag):
            success_count += 1

    print(f"\nSummary: Successfully deleted {success_count}/{len(selected_tags)} tag(s).")


if __name__ == "__main__":
    main()
