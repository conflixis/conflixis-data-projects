'use client';

import React from 'react';
import Image from 'next/image';
import { Badge } from '@workspace/ui/components/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@workspace/ui/components/card';
import { Separator } from '@workspace/ui/components/separator';
import { 
  Loader2, 
  Heart, 
  Zap, 
  Sun, 
  Moon, 
  Star,
  Cloud,
  Sparkles,
  Waves,
  Activity,
  BarChart3,
  TrendingUp,
  Circle,
  Square,
  Triangle,
  Hexagon,
  Pentagon,
  Octagon,
  AlertCircle,
} from 'lucide-react';

export function AnimationsShowcase() {
  return (
    <div className="space-y-8">
      {/* Logo Animations */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Logo Animations</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Breathing Effect</CardTitle>
              <CardDescription>Gentle pulse animation</CardDescription>
            </CardHeader>
            <CardContent className="flex justify-center items-center h-32">
              <div className="animate-pulse-slow">
                <div className="w-20 h-20 bg-conflixis-green rounded-lg flex items-center justify-center">
                  <span className="text-white font-bold text-2xl">C</span>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Bounce Effect</CardTitle>
              <CardDescription>Bouncing animation</CardDescription>
            </CardHeader>
            <CardContent className="flex justify-center items-center h-32">
              <div className="animate-bounce">
                <div className="w-20 h-20 bg-conflixis-gold rounded-lg flex items-center justify-center">
                  <span className="text-white font-bold text-2xl">C</span>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Spin Effect</CardTitle>
              <CardDescription>Continuous rotation</CardDescription>
            </CardHeader>
            <CardContent className="flex justify-center items-center h-32">
              <div className="animate-spin-slow">
                <div className="w-20 h-20 bg-conflixis-blue rounded-lg flex items-center justify-center">
                  <span className="text-white font-bold text-2xl">C</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      <Separator />

      {/* Icon Animations */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Icon Animations</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6 flex flex-col items-center">
              <Loader2 className="h-8 w-8 animate-spin text-conflixis-green mb-2" />
              <p className="text-xs text-center">Spinner</p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="pt-6 flex flex-col items-center">
              <Heart className="h-8 w-8 animate-pulse text-conflixis-red mb-2" />
              <p className="text-xs text-center">Pulse</p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="pt-6 flex flex-col items-center">
              <Star className="h-8 w-8 animate-ping text-conflixis-gold mb-2" />
              <p className="text-xs text-center">Ping</p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="pt-6 flex flex-col items-center">
              <Zap className="h-8 w-8 animate-bounce text-conflixis-blue mb-2" />
              <p className="text-xs text-center">Bounce</p>
            </CardContent>
          </Card>
        </div>
      </div>

      <Separator />

      {/* Custom Animations */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Custom Animations</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Wave Effect</CardTitle>
              <CardDescription>Smooth wave animation</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex justify-center space-x-1">
                <div className="w-3 h-16 bg-conflixis-green rounded animate-wave-1"></div>
                <div className="w-3 h-16 bg-conflixis-light-green rounded animate-wave-2"></div>
                <div className="w-3 h-16 bg-conflixis-gold rounded animate-wave-3"></div>
                <div className="w-3 h-16 bg-conflixis-blue rounded animate-wave-4"></div>
                <div className="w-3 h-16 bg-conflixis-red rounded animate-wave-5"></div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Gradient Shift</CardTitle>
              <CardDescription>Color transition effect</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-16 rounded-lg animate-gradient-shift bg-gradient-to-r from-conflixis-green via-conflixis-gold to-conflixis-blue"></div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Glow Effect</CardTitle>
              <CardDescription>Pulsing glow animation</CardDescription>
            </CardHeader>
            <CardContent className="flex justify-center">
              <div className="animate-glow w-20 h-20 bg-conflixis-green rounded-full"></div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Shake Effect</CardTitle>
              <CardDescription>Attention-grabbing shake</CardDescription>
            </CardHeader>
            <CardContent className="flex justify-center">
              <div className="animate-shake w-20 h-20 bg-conflixis-red rounded-lg flex items-center justify-center">
                <AlertCircle className="h-10 w-10 text-white" />
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      <Separator />

      {/* Loading States */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Loading States</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Skeleton Pulse</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              <div className="h-4 bg-gray-200 rounded animate-pulse"></div>
              <div className="h-4 bg-gray-200 rounded animate-pulse w-3/4"></div>
              <div className="h-4 bg-gray-200 rounded animate-pulse w-1/2"></div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Dots Loading</CardTitle>
            </CardHeader>
            <CardContent className="flex justify-center items-center h-20">
              <div className="flex space-x-2">
                <div className="w-3 h-3 bg-conflixis-green rounded-full animate-pulse-dot-1"></div>
                <div className="w-3 h-3 bg-conflixis-green rounded-full animate-pulse-dot-2"></div>
                <div className="w-3 h-3 bg-conflixis-green rounded-full animate-pulse-dot-3"></div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Progress Bar</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
                <div className="bg-conflixis-green h-2 rounded-full animate-progress"></div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      <Separator />

      {/* Shape Morphing */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Shape Morphing</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6 flex justify-center">
              <div className="w-16 h-16 bg-conflixis-green animate-morph-circle"></div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="pt-6 flex justify-center">
              <div className="w-16 h-16 bg-conflixis-gold animate-rotate-scale"></div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="pt-6 flex justify-center">
              <div className="w-16 h-16 bg-conflixis-blue rounded-lg animate-flip-3d"></div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="pt-6 flex justify-center">
              <div className="w-16 h-16 bg-conflixis-red animate-slide-fade"></div>
            </CardContent>
          </Card>
        </div>
      </div>

      <Separator />

      {/* Interactive Hover Effects */}
      <div>
        <h3 className="text-lg font-semibold mb-4">Hover Effects</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card className="group cursor-pointer">
            <CardHeader>
              <CardTitle className="text-sm">Scale on Hover</CardTitle>
            </CardHeader>
            <CardContent className="flex justify-center">
              <div className="w-20 h-20 bg-conflixis-green rounded-lg transition-transform duration-300 group-hover:scale-110"></div>
            </CardContent>
          </Card>

          <Card className="group cursor-pointer">
            <CardHeader>
              <CardTitle className="text-sm">Rotate on Hover</CardTitle>
            </CardHeader>
            <CardContent className="flex justify-center">
              <div className="w-20 h-20 bg-conflixis-gold rounded-lg transition-transform duration-300 group-hover:rotate-180"></div>
            </CardContent>
          </Card>

          <Card className="group cursor-pointer">
            <CardHeader>
              <CardTitle className="text-sm">Shadow Lift</CardTitle>
            </CardHeader>
            <CardContent className="flex justify-center">
              <div className="w-20 h-20 bg-conflixis-blue rounded-lg transition-all duration-300 group-hover:shadow-xl group-hover:-translate-y-1"></div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* CSS Animation Styles */}
      <style jsx>{`
        @keyframes wave-1 {
          0%, 100% { transform: scaleY(1); }
          50% { transform: scaleY(0.5); }
        }
        @keyframes wave-2 {
          0%, 100% { transform: scaleY(1); }
          50% { transform: scaleY(0.5); }
        }
        @keyframes wave-3 {
          0%, 100% { transform: scaleY(1); }
          50% { transform: scaleY(0.5); }
        }
        @keyframes wave-4 {
          0%, 100% { transform: scaleY(1); }
          50% { transform: scaleY(0.5); }
        }
        @keyframes wave-5 {
          0%, 100% { transform: scaleY(1); }
          50% { transform: scaleY(0.5); }
        }
        @keyframes gradient-shift {
          0%, 100% { background-position: 0% 50%; }
          50% { background-position: 100% 50%; }
        }
        @keyframes glow {
          0%, 100% { 
            box-shadow: 0 0 5px rgba(12, 52, 58, 0.5), 0 0 20px rgba(12, 52, 58, 0.3);
          }
          50% { 
            box-shadow: 0 0 20px rgba(12, 52, 58, 0.8), 0 0 40px rgba(12, 52, 58, 0.5);
          }
        }
        @keyframes shake {
          0%, 100% { transform: translateX(0); }
          10%, 30%, 50%, 70%, 90% { transform: translateX(-5px); }
          20%, 40%, 60%, 80% { transform: translateX(5px); }
        }
        @keyframes pulse-dot-1 {
          0%, 80%, 100% { opacity: 0; }
          40% { opacity: 1; }
        }
        @keyframes pulse-dot-2 {
          0%, 80%, 100% { opacity: 0; }
          50% { opacity: 1; }
        }
        @keyframes pulse-dot-3 {
          0%, 80%, 100% { opacity: 0; }
          60% { opacity: 1; }
        }
        @keyframes progress {
          0% { width: 0%; }
          100% { width: 100%; }
        }
        @keyframes morph-circle {
          0%, 100% { border-radius: 50%; }
          25% { border-radius: 25%; }
          50% { border-radius: 0%; }
          75% { border-radius: 25%; }
        }
        @keyframes rotate-scale {
          0% { transform: rotate(0deg) scale(1); }
          50% { transform: rotate(180deg) scale(0.8); }
          100% { transform: rotate(360deg) scale(1); }
        }
        @keyframes flip-3d {
          0% { transform: perspective(400px) rotateY(0); }
          100% { transform: perspective(400px) rotateY(360deg); }
        }
        @keyframes slide-fade {
          0%, 100% { opacity: 1; transform: translateX(0); }
          50% { opacity: 0.3; transform: translateX(20px); }
        }
        @keyframes spin-slow {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
        @keyframes pulse-slow {
          0%, 100% {
            opacity: 1;
            transform: scale(1);
          }
          50% {
            opacity: 0.8;
            transform: scale(0.95);
          }
        }

        .animate-wave-1 { animation: wave-1 1s ease-in-out infinite; }
        .animate-wave-2 { animation: wave-2 1s ease-in-out 0.1s infinite; }
        .animate-wave-3 { animation: wave-3 1s ease-in-out 0.2s infinite; }
        .animate-wave-4 { animation: wave-4 1s ease-in-out 0.3s infinite; }
        .animate-wave-5 { animation: wave-5 1s ease-in-out 0.4s infinite; }
        .animate-gradient-shift { 
          animation: gradient-shift 3s ease infinite;
          background-size: 200% 200%;
        }
        .animate-glow { animation: glow 2s ease-in-out infinite; }
        .animate-shake { animation: shake 0.5s ease-in-out infinite; }
        .animate-pulse-dot-1 { animation: pulse-dot-1 1.4s ease-in-out infinite; }
        .animate-pulse-dot-2 { animation: pulse-dot-2 1.4s ease-in-out infinite; }
        .animate-pulse-dot-3 { animation: pulse-dot-3 1.4s ease-in-out infinite; }
        .animate-progress { animation: progress 2s ease-in-out infinite; }
        .animate-morph-circle { animation: morph-circle 4s ease-in-out infinite; }
        .animate-rotate-scale { animation: rotate-scale 3s ease-in-out infinite; }
        .animate-flip-3d { animation: flip-3d 3s ease-in-out infinite; }
        .animate-slide-fade { animation: slide-fade 2s ease-in-out infinite; }
        .animate-spin-slow { animation: spin-slow 3s linear infinite; }
        .animate-pulse-slow { animation: pulse-slow 3s ease-in-out infinite; }
      `}</style>
    </div>
  );
}