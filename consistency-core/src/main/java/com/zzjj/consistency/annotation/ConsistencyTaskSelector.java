package com.zzjj.consistency.annotation;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author zengjin
 * @date 2023/11/19
 **/
public class ConsistencyTaskSelector implements ImportSelector {

    @Override
    public String[] selectImports(final AnnotationMetadata annotationMetadata) {
        return new String[] {ComponentScanConfig.class.getName()};
    }

}
