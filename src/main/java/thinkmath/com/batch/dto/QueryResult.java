package thinkmath.com.batch.dto;

import java.util.List;
import java.util.Map;

public record QueryResult(List<Map> response, long executionTimeMs, long totalHits) {}
