import { useState, useEffect } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
} from 'chart.js';
import { Bar, Line, Pie } from 'react-chartjs-2';

// Register chart components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
  ArcElement
);

// Define TypeScript interfaces for API responses
interface ScoreBucket {
  bucket: string;
  count: number;
}

interface PassRate {
  task: string;
  avg_score: number;
  attempts: number;
}

interface TimelineEntry {
  date: string;
  submissions: number;
}

interface GroupPerformance {
  group: string;
  avg_score: number;
  students: number;
}

interface Item {
  id: number;
  type: string;
  title: string;
  created_at: string;
}

const DASHBOARD_LABS = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05'];

const Dashboard = () => {
  const [selectedLab, setSelectedLab] = useState<string>('lab-04');
  const [scoresData, setScoresData] = useState<ScoreBucket[]>([]);
  const [passRatesData, setPassRatesData] = useState<PassRate[]>([]);
  const [timelineData, setTimelineData] = useState<TimelineEntry[]>([]);
  const [groupsData, setGroupsData] = useState<GroupPerformance[]>([]);
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch API key from localStorage
  const apiKey = localStorage.getItem('api_key');

  useEffect(() => {
    const fetchData = async () => {
      if (!apiKey) {
        setError('API key not found. Please connect first.');
        setLoading(false);
        return;
      }

      setLoading(true);
      setError(null);

      try {
        // Fetch all data concurrently
        const [scoresRes, passRatesRes, timelineRes, groupsRes, itemsRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${selectedLab}`, {
            headers: { Authorization: `Bearer ${apiKey}` },
          }),
          fetch(`/analytics/pass-rates?lab=${selectedLab}`, {
            headers: { Authorization: `Bearer ${apiKey}` },
          }),
          fetch(`/analytics/timeline?lab=${selectedLab}`, {
            headers: { Authorization: `Bearer ${apiKey}` },
          }),
          fetch(`/analytics/groups?lab=${selectedLab}`, {
            headers: { Authorization: `Bearer ${apiKey}` },
          }),
          fetch('/items/', {
            headers: { Authorization: `Bearer ${apiKey}` },
          }),
        ]);

        // Check for errors
        if (!scoresRes.ok || !passRatesRes.ok || !timelineRes.ok || !groupsRes.ok || !itemsRes.ok) {
          throw new Error('One or more API requests failed');
        }

        // Parse JSON responses
        const [scores, passRates, timeline, groups, itemsData] = await Promise.all([
          scoresRes.json(),
          passRatesRes.json(),
          timelineRes.json(),
          groupsRes.json(),
          itemsRes.json(),
        ]);

        setScoresData(scores);
        setPassRatesData(passRates);
        setTimelineData(timeline);
        setGroupsData(groups);
        setItems(itemsData);
      } catch (err) {
        console.error('Error fetching dashboard data:', err);
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedLab, apiKey]);

  // Prepare chart data
  const scoresChartData = {
    labels: scoresData.map(bucket => bucket.bucket),
    datasets: [
      {
        label: 'Number of Submissions',
        data: scoresData.map(bucket => bucket.count),
        backgroundColor: 'rgba(54, 162, 235, 0.5)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  };

  const timelineChartData = {
    labels: timelineData.map(entry => entry.date),
    datasets: [
      {
        label: 'Submissions',
        data: timelineData.map(entry => entry.submissions),
        fill: false,
        borderColor: 'rgb(75, 192, 192)',
        tension: 0.1,
      },
    ],
  };

  const passRatesChartData = {
    labels: passRatesData.map(item => item.task),
    datasets: [
      {
        label: 'Average Score',
        data: passRatesData.map(item => item.avg_score),
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
        borderColor: 'rgba(255, 99, 132, 1)',
        borderWidth: 1,
      },
    ],
  };

  if (loading) {
    return <div className="dashboard">Loading dashboard data...</div>;
  }

  if (error) {
    return <div className="dashboard">Error: {error}</div>;
  }

  return (
    <div className="dashboard">
      <header className="app-header">
        <h1>Analytics Dashboard</h1>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab: </label>
          <select
            id="lab-select"
            value={selectedLab}
            onChange={(e) => setSelectedLab(e.target.value)}
          >
            {DASHBOARD_LABS.map(lab => (
              <option key={lab} value={lab}>{lab}</option>
            ))}
          </select>
        </div>
      </header>

      <div className="charts-container">
        <div className="chart-wrapper">
          <h2>Score Distribution</h2>
          <Bar data={scoresChartData} options={{ responsive: true }} />
        </div>

        <div className="chart-wrapper">
          <h2>Submissions Over Time</h2>
          <Line data={timelineChartData} options={{ responsive: true }} />
        </div>

        <div className="chart-wrapper">
          <h2>Pass Rates by Task</h2>
          <Bar data={passRatesChartData} options={{ responsive: true }} />
        </div>

        <div className="chart-wrapper">
          <h2>Group Performance</h2>
          <table className="data-table">
            <thead>
              <tr>
                <th>Group</th>
                <th>Avg. Score</th>
                <th>Students</th>
              </tr>
            </thead>
            <tbody>
              {groupsData.map((group, index) => (
                <tr key={index}>
                  <td>{group.group}</td>
                  <td>{group.avg_score.toFixed(1)}</td>
                  <td>{group.students}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;