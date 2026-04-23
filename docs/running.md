# Running

## Probe

```bash
indc --probe
```

## Build the locality crosswalk

```bash
indc --validate
```

## Build the public bundle

```bash
indc build-public-bundle
```

## Validate the bundle

```bash
indc validate-public-bundle
```

`pr-agent-context` must stay on floating `v4` in both workflow `uses:` refs and `tool_ref`. Do not replace either with a SHA or exact `v4.x.y` tag.
