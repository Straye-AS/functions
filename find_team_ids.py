#!/usr/bin/env python3
"""
Find Team IDs Script

This script helps you find Microsoft Teams IDs by searching for team names.
You can search for multiple teams at once.

Usage:
    python find_team_ids.py

The script will prompt you to enter team names (or partial names) to search for.
"""

import asyncio
import logging
import sys
import os
import json
from typing import List, Dict

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# Load environment variables from local.settings.json
def load_local_settings():
    """Load environment variables from local.settings.json"""
    try:
        with open("local.settings.json", "r") as f:
            settings = json.load(f)
            for key, value in settings.get("Values", {}).items():
                os.environ[key] = value
        print("✅ Loaded environment variables from local.settings.json")
    except FileNotFoundError:
        print("⚠️  local.settings.json not found - using system environment variables")
    except Exception as e:
        print(f"⚠️  Error loading local.settings.json: {e}")


# Load settings before importing our modules
load_local_settings()

from teamsplanner.get_teams import TeamsProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TeamIDFinder:
    """Find Microsoft Teams IDs by name search"""

    def __init__(self):
        self.processor = TeamsProcessor()
        self.all_teams = []

    async def initialize(self):
        """Initialize the teams processor and fetch all teams"""
        try:
            await self.processor.initialize()
            print("✅ Team ID Finder initialized successfully")

            # Fetch all teams
            print("🔍 Fetching all teams from Microsoft Graph...")
            await self._fetch_all_teams()
            print(f"📊 Found {len(self.all_teams)} total teams in organization\n")

        except Exception as e:
            print(f"❌ Failed to initialize: {str(e)}")
            raise

    async def _fetch_all_teams(self):
        """Fetch all teams from Microsoft Graph"""
        teams_response = await self.processor.graph_client.teams.get()

        while teams_response:
            if teams_response.value:
                for team in teams_response.value:
                    self.all_teams.append(
                        {
                            "name": team.display_name,
                            "id": team.id,
                            "visibility": str(team.visibility).replace(
                                "TeamVisibilityType.", ""
                            ),
                            "archived": team.is_archived or False,
                        }
                    )

            if teams_response.odata_next_link:
                teams_response = await self.processor.graph_client.teams.with_url(
                    teams_response.odata_next_link
                ).get()
            else:
                break

    def search_teams(self, search_term: str) -> List[Dict]:
        """Search for teams by name (case-insensitive, partial match)"""
        search_term_lower = search_term.lower().strip()

        if not search_term_lower:
            return []

        matches = []
        for team in self.all_teams:
            if search_term_lower in team["name"].lower():
                matches.append(team)

        return matches

    def display_search_results(self, search_term: str, matches: List[Dict]):
        """Display search results in a formatted way"""
        print(f"🔍 Search results for: '{search_term}'")
        print("=" * 60)

        if not matches:
            print("❌ No teams found matching your search term")
            return

        print(f"✅ Found {len(matches)} matching team(s):\n")

        for i, team in enumerate(matches, 1):
            status = "🗄️  ARCHIVED" if team["archived"] else "✅ ACTIVE"
            visibility = (
                "🔒 Private" if team["visibility"] == "Private" else "🌐 Public"
            )

            print(f"{i}. Team: {team['name']}")
            print(f"   ID: {team['id']}")
            print(f"   Status: {status}")
            print(f"   Visibility: {visibility}")
            print()

    def export_results(self, all_results: Dict):
        """Export all search results to a file"""
        try:
            with open("team_search_results.json", "w", encoding="utf-8") as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            print("💾 Results exported to 'team_search_results.json'")
        except Exception as e:
            print(f"⚠️  Could not export results: {e}")


async def main():
    """Main function"""
    finder = TeamIDFinder()

    try:
        # Initialize
        await finder.initialize()

        print("🔍 Team ID Finder")
        print("=" * 40)
        print("Search for Microsoft Teams by name to get their IDs.")
        print("Enter partial team names to find matches.")
        print("Type 'quit' or 'exit' to finish.\n")

        all_results = {}

        # Search loop
        while True:
            search_term = input(
                "Enter team name to search (or 'quit' to exit): "
            ).strip()

            if search_term.lower() in ["quit", "exit", ""]:
                break

            if search_term:
                matches = finder.search_teams(search_term)
                finder.display_search_results(search_term, matches)

                # Store results
                all_results[search_term] = matches

                print("-" * 60)

        # Export results if any searches were performed
        if all_results:
            print(f"\n📋 Search Summary:")
            print("=" * 40)

            total_matches = sum(len(matches) for matches in all_results.values())
            print(f"Total searches: {len(all_results)}")
            print(f"Total teams found: {total_matches}")

            export_choice = (
                input("\n💾 Export results to JSON file? (y/N): ").strip().lower()
            )
            if export_choice in ["y", "yes"]:
                finder.export_results(all_results)

        print("\n👋 Team ID search completed")

    except KeyboardInterrupt:
        print("\n\n⚠️  Search interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        logging.error(f"Fatal error in main: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
