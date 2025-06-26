# Disaster Recovery Protocol
Recover the production database after catastrophic failure from a backup. This method is for restoring the entire database.

See the [official documentation](https://cloud.google.com/firestore/docs/backups?hl=en) for more information.

## Alternatives

### PITR (Point In Time Recovery)
If you need to restore a single collection or document, you can use PITR (Point In Time Recovery) to restore the database to a specific point in time. This is useful if you need to restore a single collection or document that was deleted or corrupted. See the [official documentation](https://cloud.google.com/firestore/docs/pitr?hl=en) for more information.

### Import from scheduled backup
There are also backups stored in Google Cloud Storage that are copied daily from a Firebase scheduled function (`web/functions/index`). These backups can also be used to recover the entire database. See the [official documentation](https://cloud.google.com/firestore/docs/manage-data/export-import?hl=en) for more information.

### Recover to a new database
If you want to recover the database to a new database, you can use the `gcloud alpha firestore databases restore` command to restore the database to a new database. This is useful if you want to recover the database to a new database without affecting the existing database. Then switch over to the new database after the restore is complete. This method requires a code push to update the database name in the code.

## Prerequisites
- Google cloud permissions
- Firebase permissions and CLI installed

## Restore process
1. Set project to prod
```bash
gcloud config set project conflixis-web
```

2. Describe backup schedules
```bash
gcloud alpha firestore backups schedules list \
--database='(default)'
```

3. List backups
```bash
gcloud alpha firestore backups list \
--format="table(name, database, state)"
```

4. Describe backup
```bash
gcloud alpha firestore backups describe \
--location=nam5 \
--backup=BACKUP_ID
```

5. Delete (default) database in cloud firebase console
We have to first delete the default database so we can restore back to it. After deletion, we have to wait 5 minutes before we can create a new database with the same name. Can be done from either cloud.google.com or firebase.google.com.

6. Restore backup to (default) database
```bash
gcloud alpha firestore databases restore \
--source-backup=projects/conflixis-web/locations/nam5/backups/BACKUP_ID \
--destination-database='(default)'
```

7. Check progress of job
```bash
gcloud firestore operations describe JOB_ID
```

8. Deploy prod. on local dev mono-repo
```bash
$ cd /web
$ npm run deploy:prod
```

9. Reschedule backups
```bash
gcloud alpha firestore backups schedules create \
--database='(default)' \
--recurrence=daily \
--retention=7d
```

Changelog
- 2024-05-18 - Restoration test successful - Joseph Bergen
