from pydantic import BaseModel, Field


class TriageDecision(BaseModel):
    passes: bool = Field(
        description=(
            "True if the text contains any fact, opinion, or experience about the target product. "
            "False if the text contains only a question with no embedded signal, a purely tangential "
            "mention, or off-topic noise."
        )
    )
    reasoning: str = Field(description="One sentence explaining what signal was found (or why none was found).")


def get_entity_triage_prompt(raw_mention: str, text: str, parent_text: str) -> str:
    return f"""You are a signal quality gate for a consumer product database.

Your sole objective is to determine if a piece of text contains any meaningful signal about a specific product — a fact, opinion, or experience that would help someone evaluate whether it is worth buying long-term.

Target Product: {raw_mention}

PASS (passes = true) if the text contains ANY of the following about the target product, regardless of sentiment:
- A first-hand ownership or usage experience ("I bought it", "I've had it for years", "I use it daily")
- A quality, durability, or performance assessment ("it's bulletproof", "it fell apart", "still going strong")
- A failure or defect report, even embedded inside a question ("my X broke after 2 years, how do I fix it?")
- A direct comparison to another product
- A recommendation or anti-recommendation backed by any stated reason

FAIL (passes = false) if the text contains ONLY:
- A question about the product with no embedded fact or opinion ("has anyone tried X?", "where can I buy X?")
- An incidental mention with no assessment ("I saw X at the store")
- Subreddit meta, bot responses, or off-topic content

Text to evaluate:
{text}

Parent Context (submission title/body for reference):
{parent_text}"""
