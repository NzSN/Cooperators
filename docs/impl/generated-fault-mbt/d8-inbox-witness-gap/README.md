# Expiry witness gap

The fixed-input mutation gate rejected a regenerated correct-policy witness because it expired only message 1, then delivered live message 2 and acknowledged cursor 2. The `expired-cursor` mutant only differs on an empty delivery, so that witness did not distinguish the defect. This is a coverage failure, not a Rust reproduction.

The retained TraceProbe contains the exact actions/observations, and check.log shows that the mutant conformed. Corpus generation failed before publishing its new manifest. The canonical NoInboxCapacityRecovery goal was strengthened to require both capacity records expired and an empty last delivery before the successful capacity-recovery send. The replacement model matrix and fixed-input certificates are retained separately.
