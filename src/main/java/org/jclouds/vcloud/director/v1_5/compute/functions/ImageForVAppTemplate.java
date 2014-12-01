/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jclouds.vcloud.director.v1_5.compute.functions;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.jclouds.collect.Memoized;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.ImageBuilder;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.reference.ComputeServiceConstants;
import org.jclouds.domain.Location;
import org.jclouds.logging.Logger;
import org.jclouds.vcloud.director.v1_5.VCloudDirectorMediaType;
import org.jclouds.vcloud.director.v1_5.domain.Link;
import org.jclouds.vcloud.director.v1_5.domain.VAppTemplate;
import org.jclouds.vcloud.director.v1_5.domain.ResourceEntity.Status;
import org.jclouds.vcloud.director.v1_5.domain.dmtf.Envelope;
import org.jclouds.vcloud.director.v1_5.domain.section.OperatingSystemSection;
import org.jclouds.vcloud.director.v1_5.predicates.LinkPredicates;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;

@Singleton
public class ImageForVAppTemplate implements Function<VAppTemplate, Image> {

   private static final String CENTOS = "centos";
   private static final String UBUNTU = "ubuntu";
   private static final String SUSE = "sles";
   private static final String WINDOWS = "windows";
   private static final String UNRECOGNIZED = "unrecognized";

   @Resource
   @Named(ComputeServiceConstants.COMPUTE_LOGGER)
   public Logger logger = Logger.NULL;

   private final Function<Status, Image.Status> toPortableImageStatus;
   private final Function<VAppTemplate, Envelope> templateToEnvelope;
   private final FindLocationForResource findLocationForResource;
   private final Supplier<Set<? extends Location>> locations;

   @Inject
   protected ImageForVAppTemplate(Function<Status, Image.Status> toPortableImageStatus, Function<VAppTemplate, Envelope> templateToEnvelope,
            FindLocationForResource findLocationForResource, @Memoized Supplier<Set<? extends Location>> locations) {
      this.toPortableImageStatus = checkNotNull(toPortableImageStatus, "toPortableImageStatus");
      this.templateToEnvelope = checkNotNull(templateToEnvelope, "templateToEnvelope");
      this.findLocationForResource = checkNotNull(findLocationForResource, "findLocationForResource");
      this.locations = checkNotNull(locations, "locations");
   }

   @Override
   public Image apply(VAppTemplate from) {
      checkNotNull(from, "VAppTemplate");
      Envelope ovf = templateToEnvelope.apply(from);

      ImageBuilder builder = new ImageBuilder();
      builder.ids(from.getId());
      builder.uri(from.getHref());
      builder.name(from.getName());
      Link vdc = Iterables.find(checkNotNull(from, "from").getLinks(), LinkPredicates.typeEquals(VCloudDirectorMediaType.VDC));
      if (vdc != null) {
         builder.location(Iterables.getOnlyElement(locations.get()));
         //builder.location(vdcToLocation.apply(api.getVdcApi().get(vdc.getHref())));
      } else {
         // otherwise, it could be in a public catalog, which is not assigned to a VDC
      }
      builder.description(from.getDescription() != null ? from.getDescription() : from.getName());
      //builder.operatingSystem(CIMOperatingSystem.toComputeOs(ovf));
      OperatingSystem os;
      if (ovf.getVirtualSystem() != null) {
         os = setOsDetails(ovf.getVirtualSystem().getOperatingSystemSection());
      } else {
         os = OperatingSystem.builder().description(UNRECOGNIZED).build();
      }
      builder.operatingSystem(os);
      builder.status(toPortableImageStatus.apply(from.getStatus()));
      return builder.build();
   }

   private boolean is64bit(String osType) {
      if (osType != null) {
         return osType.contains("64");
      }
      return false;
   }

   private OperatingSystem setOsDetails(OperatingSystemSection operatingSystemSection) {
      OperatingSystem.Builder osBuilder = OperatingSystem.builder();
      if (operatingSystemSection != null) {
         String osType = operatingSystemSection.getOsType();
         String description = operatingSystemSection.getDescription();
         osBuilder.description(description);
         osBuilder.is64Bit(is64bit(osType));
         if (osType.startsWith(CENTOS)) {
            osBuilder.family(OsFamily.CENTOS).version(parseVersion(CENTOS, osType).apply(description));
         }
         else if (osType.startsWith(UBUNTU)) {
            osBuilder.family(OsFamily.UBUNTU).version(parseVersion(UBUNTU, osType).apply(description));
         }
         else if (osType.startsWith(WINDOWS)) {
            osBuilder.family(OsFamily.WINDOWS).version(parseVersion(WINDOWS, osType).apply(description));
         }
         else if (osType.startsWith(SUSE)) {
            osBuilder.family(OsFamily.SUSE).version(parseVersion(SUSE, osType).apply(description));
         }
      }
      return osBuilder.build();
   }

   private Function<String, String> parseVersion(final String osFamily, final String osType) {
      return new Function<String, String>() {

         @Override
         public String apply(final String description) {
            if (osType != null) {
               if (osType.contains("_")) {
                  return Iterables.get(Splitter.on("_").split(osType), 0).substring(osFamily.indexOf(osFamily) +
                          osFamily.length()).trim();
               }
            }
            if (description != null) {
               String stripped = description.contains(" (") ? description.substring(0,
                       description.indexOf(" (")) : description;
               if (stripped.toLowerCase().contains(osFamily)) {
                  return stripped.substring(stripped.toLowerCase().indexOf(osFamily) + osFamily.length()).trim();
               }
            }
            return null;
         }

      };
   }

}
