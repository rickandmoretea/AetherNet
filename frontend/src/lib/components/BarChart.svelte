<script lang="ts">
    import { onMount, afterUpdate } from 'svelte';
    import Chart from 'chart.js/auto';
    import type { TrafficData } from '$lib/api';

    export let data: TrafficData[] = [];
    export let title: string;
    export let xAxisLabel: string;
    export let yAxisLabel: string;

    let canvas: HTMLCanvasElement;
    let chart: Chart;

    onMount(() => {
        chart = new Chart(canvas, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [{
                    label: title,
                    data: [],
                    backgroundColor: 'rgba(75, 192, 192, 0.6)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: xAxisLabel
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: yAxisLabel
                        },
                        beginAtZero: true
                    }
                }
            }
        });
    });

    afterUpdate(() => {
        if (chart && data) {
            chart.data.labels = data.map(d => d.country || d.as_number?.toString() || 'Unknown');
            chart.data.datasets[0].data = data.map(d => d.traffic);
            chart.update();
        }
    });
</script>

<canvas bind:this={canvas}></canvas>