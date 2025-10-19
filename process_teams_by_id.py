#!/usr/bin/env python3
"""
Process Teams by ID Script

This script processes Microsoft Teams by their exact Team IDs.
You can enter multiple Team IDs to process them all at once.

Usage:
    python process_teams_by_id.py

Or with command line arguments:
    python process_teams_by_id.py "team-id-1" "team-id-2" "team-id-3"

The script will create planners for each team based on your template.
"""

import asyncio
import logging
import sys
import os
import json
from typing import List, Dict, Optional

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
from teamsplanner.create_planner import PlannerTemplateManager

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TeamProcessorByID:
    """Process Microsoft Teams by their exact IDs"""

    def __init__(self):
        self.processor = TeamsProcessor()
        self.planner_manager = None
        self.results = []

    async def initialize(self):
        """Initialize the teams processor"""
        try:
            await self.processor.initialize()
            self.planner_manager = self.processor.planner_manager
            print("✅ Team Processor by ID initialized successfully")
        except Exception as e:
            print(f"❌ Failed to initialize: {str(e)}")
            raise

    async def get_team_by_id(self, team_id: str) -> Optional[Dict]:
        """Get team information by ID"""
        try:
            # Get team details from Graph API
            team = await self.processor.graph_client.teams.by_team_id(team_id).get()

            if team:
                return {
                    "id": team.id,
                    "name": team.display_name,
                    "visibility": str(team.visibility).replace(
                        "TeamVisibilityType.", ""
                    ),
                    "archived": team.is_archived or False,
                    "team_object": team,
                }
            else:
                return None

        except Exception as e:
            logging.error(f"Error fetching team {team_id}: {str(e)}")
            return None

    async def process_team_by_id(self, team_id: str) -> Dict:
        """Process a single team by its ID"""
        print(f"🎯 Processing team ID: {team_id}")
        print("-" * 50)

        # Get team information
        team_info = await self.get_team_by_id(team_id)

        if not team_info:
            result = {
                "team_id": team_id,
                "status": "not_found",
                "message": "Team not found or access denied",
            }
            print(f"❌ Team not found: {team_id}")
            return result

        team_name = team_info["name"]
        print(f"📋 Team Name: {team_name}")
        print(f"🔒 Visibility: {team_info['visibility']}")

        # Check if archived
        if team_info["archived"]:
            result = {
                "team_id": team_id,
                "team_name": team_name,
                "status": "skipped",
                "message": "Team is archived",
            }
            print(f"⚠️  Team is archived - skipping")
            return result

        # Check if already processed
        if team_id in self.processor.processed_team_ids:
            result = {
                "team_id": team_id,
                "team_name": team_name,
                "status": "already_processed",
                "message": "Team was already processed",
            }
            print(f"ℹ️  Team already processed - skipping")
            return result

        # Process the team
        try:
            print(f"🚀 Creating planner for: {team_name}")

            # Create planner using the planner manager
            planner_result = await self.planner_manager.create_planner_for_team(
                team_id, team_name
            )

            if planner_result:
                # Mark as processed
                await self.processor.mark_team_as_processed(team_id)
                self.processor.processed_team_ids.add(team_id)

                result = {
                    "team_id": team_id,
                    "team_name": team_name,
                    "status": "success",
                    "message": "Planner created successfully",
                    "planner_result": planner_result,
                }
                print(f"✅ Successfully processed: {team_name}")

            else:
                result = {
                    "team_id": team_id,
                    "team_name": team_name,
                    "status": "failed",
                    "message": "Failed to create planner",
                }
                print(f"❌ Failed to create planner for: {team_name}")

            return result

        except Exception as e:
            result = {
                "team_id": team_id,
                "team_name": team_name,
                "status": "error",
                "message": f"Processing error: {str(e)}",
            }
            print(f"❌ Error processing {team_name}: {str(e)}")
            return result

    async def process_multiple_teams(self, team_ids: List[str]):
        """Process multiple teams by their IDs"""
        print(f"🚀 Processing {len(team_ids)} teams by ID...")
        print("=" * 60)

        for i, team_id in enumerate(team_ids, 1):
            print(f"\n[{i}/{len(team_ids)}] Processing team...")
            result = await self.process_team_by_id(team_id.strip())
            self.results.append(result)

            # Add spacing between teams
            if i < len(team_ids):
                print()

    def print_summary(self):
        """Print processing summary"""
        print("\n" + "=" * 60)
        print("📊 PROCESSING SUMMARY")
        print("=" * 60)

        success_count = sum(1 for r in self.results if r["status"] == "success")

        # Group results by status
        status_groups = {}
        for result in self.results:
            status = result["status"]
            if status not in status_groups:
                status_groups[status] = []
            status_groups[status].append(result)

        # Display results by status
        status_emojis = {
            "success": "✅",
            "not_found": "❌",
            "skipped": "⚠️",
            "already_processed": "ℹ️",
            "failed": "❌",
            "error": "❌",
        }

        for status, results in status_groups.items():
            emoji = status_emojis.get(status, "❓")
            print(
                f"\n{emoji} {status.upper().replace('_', ' ')}: {len(results)} team(s)"
            )

            for result in results:
                team_name = result.get("team_name", "Unknown")
                team_id = result["team_id"]
                message = result.get("message", status)
                print(f"   • {team_name} ({team_id[:8]}...): {message}")

        print(
            f"\n🎯 Successfully processed {success_count} out of {len(self.results)} teams"
        )
        print("=" * 60)


async def main():
    """Main function"""
    processor = TeamProcessorByID()

    try:
        # Initialize
        await processor.initialize()

        # Check if team IDs were provided as command line arguments
        if len(sys.argv) > 1:
            team_ids = sys.argv[1:]
            print("🎯 Process Teams by ID (CLI Mode)")
            print("=" * 40)
            print(f"📝 Team IDs to process ({len(team_ids)}):")
            for i, team_id in enumerate(team_ids, 1):
                print(f"  {i}. {team_id}")
            print()

        else:
            # Interactive mode
            print("🎯 Process Teams by ID")
            print("=" * 40)
            print("Enter Microsoft Teams IDs to process them.")
            print("You can enter multiple IDs (one per line).")
            print("Type 'done' or 'quit' when finished.\n")

            team_ids = []

            while True:
                team_id = input(
                    f"Enter Team ID #{len(team_ids) + 1} (or 'done' to start): "
                ).strip()

                if team_id.lower() in ["done", "quit", "exit", ""]:
                    break

                if team_id:
                    # Basic validation - Team IDs are UUIDs
                    if len(team_id) >= 32 and "-" in team_id:
                        team_ids.append(team_id)
                        print(f"✅ Added team ID: {team_id}")
                    else:
                        print(
                            "⚠️  Invalid team ID format. Team IDs should be UUIDs (e.g., 12345678-1234-1234-1234-123456789abc)"
                        )

            if not team_ids:
                print("❌ No team IDs provided. Exiting.")
                return

            print(f"\n📝 Teams to process:")
            for i, team_id in enumerate(team_ids, 1):
                print(f"  {i}. {team_id}")

            confirm = (
                input("\n✅ Proceed with processing these teams? (y/N): ")
                .strip()
                .lower()
            )
            if confirm not in ["y", "yes"]:
                print("❌ Processing cancelled")
                return

        print(f"\n🚀 Starting processing...\n")

        # Process the teams
        await processor.process_multiple_teams(team_ids)

        # Show summary
        processor.print_summary()

    except KeyboardInterrupt:
        print("\n\n⚠️  Processing interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        logging.error(f"Fatal error in main: {str(e)}")
    finally:
        print("\n👋 Team processing completed")


if __name__ == "__main__":
    asyncio.run(main())
