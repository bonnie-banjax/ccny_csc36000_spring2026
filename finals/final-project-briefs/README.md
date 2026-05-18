# Modern Distributed Computing - Final Group Project

## Course Overview
Welcome to the final group project for Modern Distributed Computing. By now, you have explored core distributed computing models, including partial failure, time, ordering, consistency, replication, consensus, partitioning, and global snapshots.

For this final project, you will work in groups of 3 to 4 to design a massive, real-world distributed system. 

## Project Objective
Your goal is to create a comprehensive **Design Document** that outlines the architecture of your assigned system. You are not writing code for this specific assignment; instead, you are acting as Staff Software Engineers. You must deeply analyze the tradeoffs when selecting the technologies and patterns used to implement the system.

Each student is responsible for writing **2 to 3 pages** on their specific section, resulting in a cohesive 6 to 12-page group document.

## Submission Requirements
Your group must submit the design document in the `finals/` directory of your **group repository**.

The `finals/` directory must include a `README.md` that:
* identifies the project your group selected
* provides a short introduction to the system and your overall design
* includes any instructions needed to read, navigate, or present the submission
* links to the individual `.md` files submitted by each group member

Each student must submit a separate Markdown file in `finals/` for the section they were responsible for writing.

The `finals/README.md` file must clearly reference each group member's submitted `.md` file.

**Example of an individual student submission file:**

Filename example: `jane_doe_edge_ingestion_and_streaming.md`

## Assigned Projects
Your group must select one of the following three systems. Please refer to your specific project file for detailed breakdowns of individual responsibilities:
1. `Project_1_Global_Ride_Sharing.md` (e.g., Uber)
2. `Project_2_Distributed_File_Sync.md` (e.g., Dropbox)
3. `Project_3_Global_Social_Feed.md` (e.g., Twitter)

*Note: Please see `Sample_Project_VoltStream.md` for an example of the expected depth and focus on tradeoffs.*

## Class Presentation Guide
You will be presenting your architecture to the class. Your goal is **not** to read your document aloud, but to defend your technical decisions.

This presentation will serve as your **final exam**. A quiz covering the topics from the last two weeks of class will be given on the **same day** as the presentation, **before** presentations begin, so the final project presentation is **in addition to** that quiz.

**Format:** 15â€“20 Minute Presentation + 5 Minutes Q&A.

**Presentation Structure:**
1. **System Overview (2 mins):** What is the system? What is the scale? What are the core requirements?
2. **The Architecture Diagram (3 mins):** Show a clear, high-level diagram of how data flows from the client/edge to the database. Point out the boundaries between your different sections.
3. **The Tradeoff Showdown (8-10 mins):** *This is the most important part.* Each group member should take 2â€“3 minutes to present their single biggest architectural dilemma.
    * *Poor example:* "I used Kafka for the queue."
    * *Good example:* "We had to choose between a traditional message queue and a distributed log. Because we needed to fan out our data to three independent consumer groups, we chose a log. The tradeoff here is higher operational complexity in exchange for 'replayability'."
4. **Disaster Scenario / Chaos Engineering (3 mins):** Propose a realistic, severe failure (e.g., "A network partition separates our US-East database cluster from the coordination service"). Walk the class through exactly how your architecture degrades gracefully or recovers.
5. **Conclusion & Hindsight (2 mins):** If you had more time, what would you improve first?

## Grading Rubric
* **Tradeoff Analysis & Justification (40%):** Rigorous exploration of both sides of technical decisions (e.g., latency vs. consistency).
* **Application of Distributed Concepts (30%):** Accurate application of concepts covered up to Week 11 (quorums, logical clocks, MVCC, Raft, idempotency, etc.).
* **Presentation (30%):** Clear, technically grounded presentation of the architecture and effective defense of design decisions during Q&A.
