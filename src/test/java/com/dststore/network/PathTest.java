package com.dststore.network;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Path class.
 */
public class PathTest {
    
    @Test
    void testPathCreation() {
        // Arrange
        String sourceId = "replica-1";
        String targetId = "replica-2";
        
        // Act
        Path path = new Path(sourceId, targetId);
        
        // Assert
        assertThat(path.getSourceId()).isEqualTo(sourceId);
        assertThat(path.getTargetId()).isEqualTo(targetId);
        assertThat(path.toString()).isEqualTo(sourceId + " -> " + targetId);
    }
    
    @Test
    void testPathReverse() {
        // Arrange
        Path original = new Path("replica-1", "replica-2");
        
        // Act
        Path reversed = original.reverse();
        
        // Assert
        assertThat(reversed.getSourceId()).isEqualTo(original.getTargetId());
        assertThat(reversed.getTargetId()).isEqualTo(original.getSourceId());
    }
    
    @Test
    void testPathEquality() {
        // Arrange
        Path path1 = new Path("replica-1", "replica-2");
        Path path2 = new Path("replica-1", "replica-2");
        Path path3 = new Path("replica-2", "replica-1");
        
        // Assert
        assertThat(path1).isEqualTo(path2);
        assertThat(path1).isNotEqualTo(path3);
        assertThat(path1.hashCode()).isEqualTo(path2.hashCode());
        assertThat(path1.hashCode()).isNotEqualTo(path3.hashCode());
    }
} 