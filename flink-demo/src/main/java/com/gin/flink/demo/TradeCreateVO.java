package com.gin.flink.demo;


import lombok.*;

/**
* @author gin
* @date
*/
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeCreateVO {
    String userId;
    Integer totalQty;
    Long totalCent;
}
