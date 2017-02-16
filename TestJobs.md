# Windshield wiper settings

- Settings: off, low, medium, high 
    - Generate fake data
- Generate geographic distributison
    - Divide min/max over x,y coordinates
    - Tag windshield wiper output with region ID

# "Workload"

- Multiple queries running in different regions
- Compare on dense region and on a sparse region with different clustering intervals
    - Need to generate two sets of clusterings, one for each region
        - NOTE: change vehicle init query to select ID's from Xiaotian tables
        - NOTE: don't get creative on choosing a region to get target number of nodes
            - Less dense region should have approximately same number of vehicles
            - Roughly 150 vehicles per region

- Want to study separately the impact of filtering by region and grouping by region
    - Test filter by region and group by region SEPARATELY
- On top of different filter/groups, run different window sizes

## Note

- Be aware of optimizations related to batching of kafka messages, etc...
    - Consider how current counts might be "unfair"
- Mention flexibility of vehicle-to-cloud layer


# Other questions

- ABS activations, normalized by speed
- Do people slow down when windshield wipers are turned on?
- Speed vs temperature
    - Gaussian distribution for temperature
- When you batch but have reducebykey on geographic data, then you will accumulate more keys over time so reduction will be reduced
    - Function of churn from perspective of cluster head: how many vehicles of different regions does cluster head get data for in a given time interval
        - This is why it is important to have good mobility model
