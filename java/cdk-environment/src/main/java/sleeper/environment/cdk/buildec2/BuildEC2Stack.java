/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.environment.cdk.buildec2;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Instance;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.UserData;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;

import java.util.Collections;
import java.util.List;

import static sleeper.environment.cdk.config.AppParameters.VPC_ID;

public class BuildEC2Stack extends Stack {

    private final IVpc vpc;

    public BuildEC2Stack(Construct scope, StackProps props, IVpc inheritVpc) {
        super(scope, props.getStackName(), props);
        AppContext context = AppContext.of(this);
        BuildEC2Parameters params = BuildEC2Parameters.from(context);
        vpc = context.get(VPC_ID)
                .map(vpcId -> Vpc.fromLookup(this, "Vpc", VpcLookupOptions.builder().vpcId(vpcId).build()))
                .orElse(inheritVpc);
        BuildEC2Image image = params.image();

        Instance instance = Instance.Builder.create(this, "EC2")
                .vpc(vpc)
                .securityGroup(createSecurityGroup())
                .machineImage(image.machineImage())
                .instanceType(InstanceType.of(InstanceClass.T3, InstanceSize.LARGE))
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PRIVATE_WITH_EGRESS).build())
                .userData(UserData.custom(LoadUserDataUtil.userData(params)))
                .userDataCausesReplacement(true)
                .blockDevices(Collections.singletonList(image.rootBlockDevice()))
                .build();

        // Allow running CDK by assuming roles created by cdk bootstrap
        instance.getRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("sts:AssumeRole"))
                .resources(List.of("arn:aws:iam::*:role/cdk-*"))
                .build());

        CfnOutput.Builder.create(this, "LoginUser")
                .value(image.loginUser())
                .description("User to SSH into on build EC2 instance")
                .build();
        CfnOutput.Builder.create(this, "InstanceId")
                .value(instance.getInstanceId())
                .description("ID of the build EC2 instance")
                .build();
    }

    private SecurityGroup createSecurityGroup() {
        return SecurityGroup.Builder.create(this, "AllowOutbound")
                .vpc(vpc)
                .description("Allow outbound traffic")
                .allowAllOutbound(true)
                .build();
    }

}
