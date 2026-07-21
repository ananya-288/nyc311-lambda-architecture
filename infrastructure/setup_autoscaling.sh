#!/bin/bash
# NYC 311 Auto Scaling Setup Script
# Sets up EC2 Auto Scaling Group for Flask serving layer

echo "Setting up Auto Scaling Group..."

# Create launch template
aws ec2 create-launch-template \
    --launch-template-name NYC311-Flask-Template \
    --version-description "NYC311 Flask Serving Layer v1" \
    --launch-template-data '{
        "ImageId": "ami-0df80e66b6b8a0056",
        "InstanceType": "t2.micro",
        "KeyName": "vockey"
    }' \
    --region us-east-1

echo "Launch template created"

# Create Auto Scaling Group
aws autoscaling create-auto-scaling-group \
    --auto-scaling-group-name NYC311-FlaskASG \
    --launch-template LaunchTemplateName=NYC311-Flask-Template,Version='1' \
    --min-size 1 \
    --max-size 4 \
    --desired-capacity 1 \
    --vpc-zone-identifier "subnet-099731c62af86920d" \
    --region us-east-1

echo "Auto Scaling Group created"

# Create scaling policy — CPU > 60%
aws autoscaling put-scaling-policy \
    --policy-name NYC311-ScaleOutPolicy \
    --auto-scaling-group-name NYC311-FlaskASG \
    --policy-type TargetTrackingScaling \
    --target-tracking-configuration '{
        "PredefinedMetricSpecification": {
            "PredefinedMetricType": "ASGAverageCPUUtilization"
        },
        "TargetValue": 60.0
    }' \
    --region us-east-1

echo "Scaling policy created — CPU > 60% triggers scale out"
echo "Auto Scaling setup complete!"
echo ""
echo "Configuration:"
echo "  Min instances: 1"
echo "  Max instances: 4"
echo "  Scale out trigger: CPU > 60%"
echo "  Policy type: Target Tracking"