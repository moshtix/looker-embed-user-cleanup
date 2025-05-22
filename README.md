# User Embed User Cleanup

## Getting started

Install dependencies

```
npm ci
```

Run log only command with Looker API credentials to test

```
node looker-delete-users.js --base-url https://yourlookerinstance.looker.com/ --client-id yourapicredentialsclientid --client-secret yourapicredentialssecret
```

When ready to delete, run command with --force

```
node looker-delete-users.js --base-url https://yourlookerinstance.looker.com/ --client-id yourapicredentialsclientid --client-secret yourapicredentialssecret
```
