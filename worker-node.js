const express = require("express");
const app = express();

app.use(express.json());

// Load data from environment variable or use a default dataset
const DATA = process.env.DATASET ? JSON.parse(process.env.DATASET) : [];

console.log(`Worker Node started with the following dataset: ${JSON.stringify(DATA)}`);

// Endpoint to handle local search
app.get("/search", (req, res) => {
    const query = req.query.q;
    if (!query) {
        return res.status(400).json({ error: "Query parameter 'q' is required." });
    }

    // Perform case-insensitive search on the data
    const results = DATA.filter((item) => item.toLowerCase().includes(query.toLowerCase()));

    // Log the query and the results produced by this worker
    console.log(`Worker Node on port ${process.env.PORT}: Received query "${query}", Results: ${JSON.stringify(results)}`);

    res.json({ results });
});

// Health check endpoint for workers
app.get("/health", (req, res) => {
    res.status(200).send("OK");
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Worker node running on http://localhost:${PORT}`);
});
