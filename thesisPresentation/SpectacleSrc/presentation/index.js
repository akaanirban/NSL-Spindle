// Import React
import React from "react";

// Import Spectacle Core tags
import {
  BlockQuote,
  Cite,
  Deck,
  Heading,
  ListItem,
  List,
  Quote,
  Slide,
  Text
} from "spectacle";

// Import image preloader util
import preloader from "spectacle/lib/utils/preloader";

// Import theme
import createTheme from "spectacle/lib/themes/default";

// Require CSS
require("normalize.css");
require("spectacle/lib/themes/default/index.css");


const images = {
  city: require("../assets/city.jpg"),
  kat: require("../assets/kat.png"),
  logo: require("../assets/formidable-logo.svg"),
  markdown: require("../assets/markdown.png")
};

preloader(images);

const theme = createTheme({
  primary: "white",
  secondary: "#060618",
  tertiary: "#28283D",
  quartenary: "#464664"
}, {
  primary: "Helvetica",
  secondary: "Helvetica",
});

/*
 * Presentation Outline
 * - Background (v2v, data consumption)
 * - Motivation (lots of data intractible or at least undesirable to process)
 *   - Mention that we assume V2V comms are cheap, V2I not
 * - Cloud technologies: Spark, Kafka, Akka, Amazon EC2, S3 
 *      - Spend some time on waslking through how Spark jobs are written, where it is used
 * - High-level description of SPindle challenges and goals: Write Spark jobs trivially, save bandwidth, leverage cutting-edge v2v v2i tech
 *      - List some example use-cases (queries)
 * - Spindle Architecture High-Level:
 *   1) Vehicle-to-vehicle clusters
 *   2) V2I Clusterheads
 *   3) Cloud Ingestion and Spark Processing
 * - How a Spark Streaming client would be written
 *      - Focus on map and reduce functions
 * - How V2V and Clusterhead data processing works: map operations on vehicles, reduce operations on clusterheads
 * - How clusterheads can de-duplicate data and send results to cloud middleware
 * - How middleware routes data to spark jobs, how second reduce operation works, how ingested data is integrated seamlessly with rest of Spark program
 * - Simulation Architecture:
 *      - Explain goal of simulator (demonstrate data savings from clustering and map/reduce at vehicle clusters)
 *      - Explain architecture (AutoSim, World Actor, Vehicle Actors, Kafka Streams threads, middleware connection data counters)
 *      - Brief explanation of test configurations and why they were chosen (speedSum: full reduce baseline, geoMapped: reducebykey, geoFiltered: filter)
 *      - Explain Results:
 *          - Data savings for clustered vs unclustered
 *              - Explain why this makes sense (get from thesis)
 *          - Comparison across test configurations
 *              - Explain things that make sense, surprising results
 * - Conclusion:
 *      - Repeat high-level goal
 *      - Briefly re-explain architecture (VANETs, Vehicle Clusters, Middleware, Spark Streaming)
 *      - Briefly summarize results (we saved a ton of data)
 * - Future Work:
 *   - Streaming more operators to the edge
 *   - Tighter integration with Apache Spark (get involved with core SPark develoeprs to find other applications for function teleportation)
 *   - Integration with other technologies: Flink, Heron, Kafka Streams, etc...
 *   - Explore how to implement persistence for ML and thresholding applications
 *   - Explore more complex architectures depending on dynamism of clusters
 *      - Vehicles move among clusters: clusters can possibly exchange data this way
 */
export default class Presentation extends React.Component {
  render() {
    return (
      <Deck transition={["fade"]} transitionDuration={100} theme={theme} progress={'number'}>
        <Slide>
            <Heading size={1}>Spindle</Heading>
            <Heading size={4} margin="20px 0 0">Hybrid Vehicle/Cloud Architecture</Heading>
            <Text margin="20px" fit>William Rory Kronmiller, Dr. Stacy Patterson Advisor</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Background</Heading>
            <List>
                <ListItem>Newer Vehicles Expected to Generate Up To 250 GB/hr</ListItem>
                <ListItem>New Standards for V2V Communication Coming</ListItem>
            </List>
        </Slide>
        <Slide>
            <Heading size={5}>Motivation</Heading>
            <Text>TODO: add information about intractabiliity/expense of sending all data</Text>
        </Slide>
        <Slide>
            <Heading size={3}>Relevant Technologies</Heading>
        </Slide>
        <Slide>
            <Heading size={5}>Amazon Web Services</Heading>
            <Text>TODO: EC2, S3</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Apache Spark</Heading>
            <Text>text</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Akka</Heading>
            <Text>JVM Actor Model Framework</Text>
            <Text>JVM Actor Model Framework</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Apache Kafka</Heading>
            <Text>text</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Cloud IoT Platforms</Heading>
            <List>
                <ListItem>Apache Edgent (Incubator)</ListItem>
                <ListItem>AWS IoT and Greengrass</ListItem>
                <ListItem>Azure IoT</ListItem>
                <ListItem>Google, IBM, Others</ListItem>
            </List>
        </Slide>
        <Slide>
            <Heading size={3}>Spindle Goals and Challenges</Heading>
            <Heading size={6}>Improving on the State of the Art</Heading>
        </Slide>
        <Slide>
            <Heading size={5}>Challenges</Heading>
            <Text>TODO: reshash Motivation with specifics</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Use Cases</Heading>
            <Text>TODO: list some use cases</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Goal 1: Bandwidth Savings</Heading>
            <Text>Minimize Bandwidth from Vehicle to Internet:</Text>
            <Text>Leverage V2V Bandwidth to Reduce V2I Bandwidth</Text>
        </Slide>
        <Slide>
            <Heading size={5}>Goal 2: Usable System</Heading>
            <Text>Design a system that is easy to use, easy to integrate:</Text>
            <Text>Create Simple Apache Spark Interface</Text>
        </Slide>
        <Slide>
            <Heading size={3}>Spindle Architecture</Heading>
        </Slide>
        <Slide>
            <Heading size={5}>High-Level Components</Heading>
            <Text>VANET Vehicle Clusters</Text>
            <Text>VANET Cluster Heads</Text>
            <Text>Cloud: Data Ingestion, Spark Processing</Text>
        </Slide>
        <Slide>
            <Heading size={5}>TITLE</Heading>
            <Text>text</Text>
        </Slide>
      </Deck>
    );
  }
}

export class PresentationOld extends React.Component {
  render() {
    return (
      <Deck transition={["zoom", "slide"]} transitionDuration={500} theme={theme}>
        <Slide transition={["zoom"]} bgColor="primary">
          <Heading size={1} fit caps lineHeight={1} textColor="secondary">
            Spectacle Boilerplate
          </Heading>
          <Text margin="10px 0 0" textColor="tertiary" size={1} fit bold>
            open the presentation/index.js file to get started
          </Text>
        </Slide>
        <Slide transition={["fade"]} bgColor="tertiary">
          <Heading size={6} textColor="primary" caps>Typography</Heading>
          <Heading size={1} textColor="secondary">Heading 1</Heading>
          <Heading size={2} textColor="secondary">Heading 2</Heading>
          <Heading size={3} textColor="secondary">Heading 3</Heading>
          <Heading size={4} textColor="secondary">Heading 4</Heading>
          <Heading size={5} textColor="secondary">Heading 5</Heading>
          <Text size={6} textColor="secondary">Standard text</Text>
        </Slide>
        <Slide transition={["fade"]} bgColor="primary" textColor="tertiary">
          <Heading size={6} textColor="secondary" caps>Standard List</Heading>
          <List>
            <ListItem>Item 1</ListItem>
            <ListItem>Item 2</ListItem>
            <ListItem>Item 3</ListItem>
            <ListItem>Item 4</ListItem>
          </List>
        </Slide>
        <Slide transition={["fade"]} bgColor="secondary" textColor="primary">
          <BlockQuote>
            <Quote>Example Quote</Quote>
            <Cite>Author</Cite>
          </BlockQuote>
        </Slide>
      </Deck>
    );
  }
}
