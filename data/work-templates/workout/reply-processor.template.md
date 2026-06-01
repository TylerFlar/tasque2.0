# Workout Reply Processor

## Goal
Interpret a user's reply to a workout prescription, save durable training state, and queue the next Workout Generator run when the reply gives a clear training outcome.

## Context
Gather enough context to understand what the user is replying to:

- the parent workout report or artifact
- the source reply text and any uploaded files
- recent conversation messages in the Discord thread or channel
- canonical current workout state
- recent workout prescriptions, completion notes, skipped sessions, and unconfirmed prescriptions
- strength baselines, cardio baselines, equipment, and programming notes when useful for interpreting the reply

## Instructions
Determine whether the user confirmed completing the prescribed workout, completed only part of it, substituted exercises, did a different workout, did cardio only, skipped it, asked a question, or provided unrelated context.

Extract the useful training facts:

- date or implied session
- prescribed focus/session type and completed focus/session type
- exercises performed
- sets, reps, and loads
- cardio machine used, duration, speed, incline, resistance, watts, distance, intervals, heart-rate notes, or RPE
- substitutions, missed reps, skipped exercises, extra work, or changed order
- enjoyment, schedule friction, equipment friction, or other readiness notes that should affect the next prescription

Mark a workout completed only when the reply clearly says the user did the workout or gives actual completed work. A clear partial, skipped, or different-session reply is still useful state and can trigger the next generator run after it is saved. If the reply is ambiguous, save it as context and ask a brief clarifying question rather than inventing completion.

After saving a clear completed, partial, skipped, or different-session update, queue one immediate Workout Generator follow-up in the same Discord thread. Use the standard workout generator template and reusable workout context when available, then merge in the source reply, parent prescription context, completion summary, and next-focus notes. Keep the reply-routing context on the follow-up so the loop can continue after the next user reply.

## Output
Write a short acknowledgement for Discord:

**Logged**: <what was captured>
**State**: <completed | partial | skipped | different workout | unclear>
**Next**: <queued next workout, or the clarification needed>

The summary should be one short sentence naming the logged status and next focus when known.

## Save
Update workout memory after interpreting the reply:

- update canonical workout state with the latest confirmed completed session only when completion or partial completion is clear
- preserve the latest prescribed session and whether it remains unconfirmed, was completed, was partially completed, was skipped, or was replaced by different work
- record actual loads, reps, substitutions, cardio machine/settings, duration, RPE, skipped/partial work, and notes that should affect progression
- create a compact working completion log
- queue the next Workout Generator cycle only for clear completed, partial, skipped, or different-workout replies
- include compact produces metadata: completion status, session type, main lifts, cardio machine data, issue flags, next focus, and follow-up work id when queued

If the reply is unclear, do not update confirmed completion; save the note as working context and return `awaiting_user` or `blocked` with the clarification question.

## Checks
- The parent prescription was considered before interpreting the reply.
- Completion is not inferred from vague encouragement or unrelated messages.
- Actual loads/reps/cardio settings/RPE are preserved when present.
- Canonical state separates last prescribed from last confirmed completed.
- Clear workout-status replies trigger exactly one follow-up generator run.
