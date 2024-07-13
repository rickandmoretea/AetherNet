<script lang="ts"> 
    import { onMount } from 'svelte';
    import { Chart, type ChartConfiguration } from 'chart.js/auto';
    import { fetchTopAS } from '$lib/api';
    import type { TrafficData } from '$lib/api';
    
    export let startTime: string;
    export let endTime: string;

    let chart: Chart;
    let data: TrafficData[] = [];

    $: if (startTime && endTime) {
        updateChart();
    }

    async function updateChart() {
        data = await fetchTopAS(startTime, endTime);
        if (chart) {
            chart.data.labels = data.map(d => d.as_number?.toString() || 'Unknown');
            chart.data.datasets[0].data = data.map(d => d.traffic);
            chart.update();
        }
    }

    onMount(() => {
        const ctx = document.getElementById('topASChart') as HTMLCanvasElement;
        const config: ChartConfiguration = {
            type: 'bar',
            data: {
                labels: data.map(d => d.as_number?.toString || 'Unknown'),
                datasets: [{
                    label: 'Traffic by AS',
                    data: data.map(d => d.traffic),
                }]
            },
        };
        chart = new Chart(ctx, config);
    });

</script>

<canvas id="topASChart"></canvas>

