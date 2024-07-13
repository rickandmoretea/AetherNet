export const VITE_API_URL = '$env/static/public';

export interface TrafficData {
    country?: string;
    as_number?: number;
    traffic: number;
}

async function fetchData(endpoint: string, startTime: string, endTime: string): Promise<TrafficData[]> {
    const response = await fetch(`${VITE_API_URL}${endpoint}?start=${startTime}&end=${endTime}`);
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
}

export function fetchTopCountries(startTime: string, endTime: string): Promise<TrafficData[]> {
    return fetchData('/top_countries', startTime, endTime);
}

export function fetchTopAS(startTime: string, endTime: string): Promise<TrafficData[]> {
    return fetchData('/top_as', startTime, endTime);
}

export function fetchTrafficByCountry(startTime: string, endTime: string): Promise<TrafficData[]> {
    return fetchData('/traffic_by_country', startTime, endTime);
}

export function fetchTrafficByAS(startTime: string, endTime: string): Promise<TrafficData[]> {
    return fetchData('/traffic_by_as', startTime, endTime);
}