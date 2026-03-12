"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=httpx.BasicAuth(
                settings.autochecker_email, settings.autochecker_password
            ),
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch items: {response.status_code}")

        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    This function fetches all logs with pagination support.
    - Uses httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Passes HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handles pagination: keeps fetching while has_more is True
      - Uses the last log's submitted_at as the new "since" value
    - Returns the combined list of all log dicts from all pages
    """
    all_logs = []

    while True:
        params = {"limit": 500}
        if since:
            params["since"] = since.isoformat()

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=httpx.BasicAuth(
                    settings.autochecker_email, settings.autochecker_password
                ),
            )

            if response.status_code != 200:
                raise Exception(f"Failed to fetch logs: {response.status_code}")

            data = response.json()
            logs_batch = data.get("logs", [])

            if not logs_batch:
                break

            all_logs.extend(logs_batch)

            if not data.get("has_more", False):
                break

            # Use the submitted_at of the last log as the new "since" value
            last_log = logs_batch[-1]
            since = datetime.fromisoformat(
                last_log["submitted_at"].replace("Z", "+00:00")
            )

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    from sqlmodel import select

    new_items_count = 0

    # Process labs first
    lab_mapping = {}
    for item_dict in items:
        if item_dict["type"] == "lab":
            # Check if lab already exists
            existing_lab_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab", ItemRecord.title == item_dict["title"]
                )
            )
            existing_lab = existing_lab_result.first()

            if not existing_lab:
                # Create new lab
                lab_record = ItemRecord(type="lab", title=item_dict["title"])
                session.add(lab_record)
                new_items_count += 1
                lab_mapping[item_dict["lab"]] = lab_record
            else:
                lab_mapping[item_dict["lab"]] = existing_lab

    # Flush to get IDs for newly created labs
    await session.flush()

    # Process tasks
    for item_dict in items:
        if item_dict["type"] == "task":
            # Find parent lab
            parent_lab = lab_mapping.get(item_dict["lab"])
            if not parent_lab:
                continue  # Skip if parent lab doesn't exist

            # Check if task already exists
            existing_task_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "task",
                    ItemRecord.title == item_dict["title"],
                    ItemRecord.parent_id == parent_lab.id,
                )
            )
            existing_task = existing_task_result.first()

            if not existing_task:
                # Create new task
                task_record = ItemRecord(
                    type="task", title=item_dict["title"], parent_id=parent_lab.id
                )
                session.add(task_record)
                new_items_count += 1

    await session.commit()
    return new_items_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    # Build lookup from (lab_short_id, task_short_id) to item title
    item_lookup = {}
    for item in items_catalog:
        if item["type"] == "lab":
            item_lookup[(item["lab"], None)] = item["title"]
        elif item["type"] == "task":
            item_lookup[(item["lab"], item["task"])] = item["title"]

    new_interactions_count = 0

    for log in logs:
        # Find or create learner
        from sqlmodel import select

        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        learner = learner_result.first()

        if not learner:
            learner = Learner(external_id=log["student_id"], student_group=log["group"])
            session.add(learner)
            await session.commit()
            await session.refresh(learner)  # To get the ID

        # Find the matching item in the database
        item_title = item_lookup.get((log["lab"], log["task"]))
        if not item_title:
            continue  # Skip if no matching item title found

        item_result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = item_result.first()

        if not item:
            continue  # Skip if no matching item found in DB

        # Check if an InteractionLog with this external_id already exists
        existing_interaction = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log["id"])
        )
        existing_interaction = existing_interaction.first()

        if existing_interaction:
            continue  # Skip if interaction already exists (idempotent upsert)

        # Create InteractionLog
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(
                log["submitted_at"].replace("Z", "+00:00")
            ),
        )
        session.add(interaction)
        new_interactions_count += 1

    await session.commit()
    return new_interactions_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    # Step 1: Fetch items from the API and load them
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    from sqlmodel import select

    result = await session.exec(
        select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    )
    last_interaction = result.first()

    since = None
    if last_interaction:
        since = last_interaction.created_at

    # Step 3: Fetch logs since that timestamp and load them
    logs = await fetch_logs(since)
    new_records = await load_logs(logs, items, session)

    # Count total records in DB
    total_result = await session.exec(select(InteractionLog))
    total_records = len(total_result.all())

    return {"new_records": new_records, "total_records": total_records}
