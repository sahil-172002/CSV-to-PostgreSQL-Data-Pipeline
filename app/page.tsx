import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { ArrowRight, Database, FileText, Github } from "lucide-react"

export default function Home() {
  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-4xl mx-auto">
        <div className="space-y-4 text-center mb-12">
          <h1 className="text-4xl font-bold tracking-tight sm:text-5xl">CSV to PostgreSQL Data Pipeline</h1>
          <p className="text-xl text-muted-foreground">
            An efficient data pipeline to import CSV data into PostgreSQL using Python
          </p>
          <div className="flex justify-center gap-4 pt-4">
            <Button asChild>
              <Link href="https://github.com/your-username/python-csv-postgresql">
                <Github className="mr-2 h-4 w-4" />
                View on GitHub
              </Link>
            </Button>
            <Button variant="outline" asChild>
              <Link href="#demo">
                <ArrowRight className="mr-2 h-4 w-4" />
                View Demo
              </Link>
            </Button>
          </div>
        </div>

        <div className="grid gap-8 md:grid-cols-2 mb-12">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <FileText className="mr-2 h-5 w-5" />
                CSV Processing
              </CardTitle>
              <CardDescription>Efficiently process large CSV files in chunks</CardDescription>
            </CardHeader>
            <CardContent>
              <p>
                Our pipeline handles CSV files of any size by processing them in manageable chunks, preventing memory
                issues when dealing with large datasets.
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Database className="mr-2 h-5 w-5" />
                PostgreSQL Integration
              </CardTitle>
              <CardDescription>Optimized database operations</CardDescription>
            </CardHeader>
            <CardContent>
              <p>
                Uses bulk inserts and proper connection management to ensure fast and reliable data loading into
                PostgreSQL databases.
              </p>
            </CardContent>
          </Card>
        </div>

        <div className="prose prose-gray dark:prose-invert max-w-none">
          <h2 id="features">Key Features</h2>
          <ul>
            <li>Chunk-based processing for memory efficiency</li>
            <li>Automatic schema inference from CSV data</li>
            <li>Robust error handling and logging</li>
            <li>Performance monitoring</li>
            <li>Data validation and cleaning</li>
            <li>Secure credential management</li>
          </ul>

          <h2 id="installation">Installation</h2>
          <pre>
            <code>{`pip install psycopg2-binary pandas python-dotenv`}</code>
          </pre>

          <h2 id="usage">Basic Usage</h2>
          <pre>
            <code>{`from src.pipeline import DataPipeline
from src.database import DatabaseConnection
from src.config import DB_CONFIG

# Initialize
db = DatabaseConnection(DB_CONFIG)
pipeline = DataPipeline(db)

# Process a CSV file
pipeline.process_csv(
    file_path='data/sample.csv',
    table_name='employees',
    chunk_size=1000
)`}</code>
          </pre>

          <h2 id="demo">Demo</h2>
          <p>The following demo shows the pipeline processing a sample dataset of 5,000 records:</p>
          <pre>
            <code>{`2024-03-20 10:15:30 - INFO - Starting CSV import...
2024-03-20 10:15:31 - INFO - Processing chunk 1 of 5...
2024-03-20 10:15:32 - INFO - Processing chunk 2 of 5...
2024-03-20 10:15:33 - INFO - Processing chunk 3 of 5...
2024-03-20 10:15:34 - INFO - Processing chunk 4 of 5...
2024-03-20 10:15:35 - INFO - Processing chunk 5 of 5...
2024-03-20 10:15:35 - INFO - Import completed in 5.23 seconds`}</code>
          </pre>
        </div>
      </div>
    </div>
  )
}

