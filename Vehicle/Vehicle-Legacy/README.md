# NSL-Spindle
Top-Level Spindle Project

# Workshop Paper Checklist

- [x] Build Spark library 
- [ ] Build vehicle cluster simulator 
- [ ] Build cloud middleware (TODO: more specific requirements)

## Cluster Simulator Requirements

- Be able to produce realistic data
    - Realistic values lower priority
    - Need to know how much volume is being produced
        - For comparison between no batching/edge-reduction and Spindle
- Realistically simulate message loss/delay
    - Focus of project is how network affects savings
- Program behavior similar to production implementation where we are testing results
