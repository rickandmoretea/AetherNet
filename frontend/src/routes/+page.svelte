<script lang="ts">
  import { onMount } from 'svelte';
  import BarChart from '$lib/components/BarChart.svelte';
  import { fetchTopCountries, fetchTopAS, fetchTrafficByCountry, fetchTrafficByAS } from '$lib/api';
  import type { TrafficData } from '$lib/api';

  let startTime = new Date(Date.now() - 24*60*60*1000).toISOString().slice(0, -8);
  let endTime = new Date().toISOString().slice(0, -8);

  let topCountriesData: TrafficData[] = [];
  let topASData: TrafficData[] = [];
  let trafficByCountryData: TrafficData[] = [];
  let trafficByASData: TrafficData[] = [];

  async function updateData() {
      try {
          [topCountriesData, topASData, trafficByCountryData, trafficByASData] = await Promise.all([
              fetchTopCountries(startTime, endTime),
              fetchTopAS(startTime, endTime),
              fetchTrafficByCountry(startTime, endTime),
              fetchTrafficByAS(startTime, endTime)
          ]);
      } catch (error) {
          console.error('Error fetching data:', error);
      }
  }

  onMount(updateData);

  function handleTimeRangeChange() {
      updateData();
  }
</script>

<main>
  <h1>Network Traffic Dashboard</h1>

  <div class="time-range">
      <label>
          Start Time:
          <input type="datetime-local" bind:value={startTime} on:change={handleTimeRangeChange}>
      </label>
      <label>
          End Time:
          <input type="datetime-local" bind:value={endTime} on:change={handleTimeRangeChange}>
      </label>
  </div>

  <div class="charts">
      <div class="chart">
          <h2>Top Countries</h2>
          <BarChart data={topCountriesData} title="Top Countries" xAxisLabel="Country" yAxisLabel="Traffic" />
      </div>
      <div class="chart">
          <h2>Top AS</h2>
          <BarChart data={topASData} title="Top AS" xAxisLabel="AS Number" yAxisLabel="Traffic" />
      </div>
      <div class="chart">
          <h2>Traffic by Country</h2>
          <BarChart data={trafficByCountryData} title="Traffic by Country" xAxisLabel="Country" yAxisLabel="Traffic" />
      </div>
      <div class="chart">
          <h2>Traffic by AS</h2>
          <BarChart data={trafficByASData} title="Traffic by AS" xAxisLabel="AS Number" yAxisLabel="Traffic" />
      </div>
  </div>
</main>

<style>
  .time-range {
      margin-bottom: 1rem;
  }
  .charts {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
      gap: 1rem;
  }
  .chart {
      border: 1px solid #ccc;
      padding: 1rem;
      border-radius: 4px;
  }
</style>